import time
import threading
from datetime import datetime
from rich.console import Console
import sys
import select
import tty
import termios
from rich.table import Table
from rich.prompt import Prompt
from rich.text import Text
from .base import WorkflowOutput
from ..core.models import NodeStatus


class StdoutWorkflowOutput(WorkflowOutput):
    _log_name = 'console.log'

    def __init__(self, backend_url, event, logging_dir, log_level, cmd_args):
        super().__init__(backend_url, event, logging_dir, log_level, cmd_args)
        self._refresh_interval = 2
        self.__console = Console()
        self.__interactive_retry = cmd_args.interactive_retry
        self.__doubtful_mode = cmd_args.doubtful_mode
        self.known_nodes = {}
        self.user_chose_to_quit = False
        self.declined_retry_nodes = set()
        self.approved_nodes = set()
        self.console_lock = threading.Lock()
        self.stop_requested = False

    def draw_init(self):
        self._logger.debug("Initializing stdout output")
        if self.is_verify_only():
            self.__console.print("[bold yellow]Running in VERIFY ONLY mode[/]", justify="center")
        self.__console.print("\n[bold cyan]Press Ctrl+X to stop the workflow or Ctrl+C to detach from the backend[/]\n", justify="center")
        self.__console.print("[italic]Waiting for workflow to start...[/]", justify="center")

        nodes = self.api_client.get_all_nodes()
        while not nodes:
            time.sleep(1)
            nodes = self.api_client.get_all_nodes()

        # calculate first column size
        maximum_first_colum_width = 0
        if nodes:
            for node in nodes:
                if len(node['id']) > maximum_first_colum_width:
                    maximum_first_colum_width = len(node['id'])

        if maximum_first_colum_width < 17:
            self.__first_column_width = 17
        else:
            self.__first_column_width = maximum_first_colum_width

        table = Table(title="Workflow nodes")
        table.add_column("Node", justify="left", style="cyan", no_wrap=True, width=self.__first_column_width)
        table.add_column("Playbook", style="bright_magenta")
        table.add_column("Ref.", style="cyan")
        table.add_column("Started", style="green")
        table.add_column("Ended", style="green")
        table.add_column("Status")


        if nodes:
            for node in nodes:
                node_type = node.get('type')
                if node_type in ['playbook', 'info', 'checkpoint']:
                    self.known_nodes[node['id']] = node

                playbook_col = "-"
                if node_type == 'playbook':
                    playbook_col = node.get('playbook', '-')
                elif node_type == 'info':
                    playbook_col = f"[dim]({node.get('description', 'Info')})[/dim]"
                elif node_type == 'checkpoint':
                    playbook_col = f"[dim]({node.get('description', 'Checkpoint')})[/dim]"

                if node_type in ['playbook', 'info', 'checkpoint']:
                    table.add_row(
                        node['id'],
                        playbook_col,
                        node.get('reference', '-'),
                        node.get('started', ''),
                        node.get('ended', ''),
                        self._render_status(node['status'])
                    )
        self.__console.print(table)
        self.__console.print("")


        if nodes and self.__interactive_retry:
            for node in nodes:
                if node['status'] == NodeStatus.FAILED.value:
                    if node['id'] not in self.declined_retry_nodes:
                        self.handle_retry(node)

        self.__console.print("[italic]Running[/] ...", justify="center")

    def draw_step(self):
        nodes = self.api_client.get_all_nodes()
        found_failed_node_to_prompt = False
        if nodes:
            for node in nodes:
                node_id = node['id']
                if node_id in self.known_nodes and self.known_nodes[node_id]['status'] != node['status']:
                    self.print_node_status_change(node)
                    self.known_nodes[node_id] = node

                if node['status'] == NodeStatus.FAILED.value and self.__interactive_retry and node.get('type') != 'checkpoint':
                    if node['id'] not in self.declined_retry_nodes:
                        self.handle_retry(node)
                        found_failed_node_to_prompt = True

                if node['status'] == NodeStatus.AWAITING_CONFIRMATION.value:
                    if node['id'] not in self.approved_nodes:
                        if node.get('type') == 'checkpoint':
                            if self.handle_checkpoint_node(node):
                                return
                        elif self.__doubtful_mode:
                            if self.handle_doubtful_node(node):
                                return

        status_data = self.api_client.get_workflow_status()
        if status_data.get('status') == 'failed' and not found_failed_node_to_prompt:
            self.user_chose_to_quit = True


    def draw_pause(self):
        ''' Non blocking thread wait'''
        time.sleep(self._refresh_interval)

    def draw_end(self, status_data: dict = None):
        if status_data:
            errors = status_data.get('validation_errors')
            if errors:
                self.__console.print("\n[bold red]Workflow validation failed with errors:[/bold red]")
                for error in errors:
                    self.__console.print(f"- {error}")
                self.__console.print("")

        nodes = self.api_client.get_all_nodes()
        table = Table(title="Running recap")

        table.add_column("Node", justify="left", style="cyan", no_wrap=True, width=self.__first_column_width)
        table.add_column("Playbook", style="bright_magenta")
        table.add_column("Ref.", style="cyan")
        table.add_column("Started", style="green")
        table.add_column("Ended", style="green")
        table.add_column("Status")

        if nodes:
            for node in nodes:
                node_type = node.get('type')
                playbook_col = "-"
                if node_type == 'playbook':
                    playbook_col = node.get('playbook', '-')
                elif node_type == 'info':
                    playbook_col = f"[dim]({node.get('description', 'Info')})[/dim]"
                elif node_type == 'checkpoint':
                    playbook_col = f"[dim]({node.get('description', 'Checkpoint')})[/dim]"

                if node_type in ['playbook', 'info', 'checkpoint']:
                    table.add_row(
                        node['id'],
                        playbook_col,
                        node.get('reference', '-'),
                        node.get('started', ''),
                        node.get('ended', ''),
                        self._render_status(node['status'])
                    )
        self.__console.print(table)
        self.__console.print("")
        self._logger.debug("stdout output ends")

    def _render_status(self, status):
        if status == NodeStatus.RUNNING.value:
            return '[yellow]started[/]'
        elif status == NodeStatus.ENDED.value:
            return '[green]completed[/]'
        elif status == NodeStatus.FAILED.value:
            return '[bright_red]failed[/]'
        elif status == NodeStatus.NOT_STARTED.value:
            return '[white]not started[/]'
        elif status == NodeStatus.SKIPPED.value:
            return '[cyan]skipped[/]'
        elif status == NodeStatus.STOPPED.value:
            return '[red]stopped[/]'
        elif status == NodeStatus.AWAITING_CONFIRMATION.value:
            return '[bold yellow]awaiting confirmation[/]'
        else:
            return 'unknown'

    def handle_doubtful_node(self, node):
        y_or_n = ''
        self.__console.line()
        self.__console.rule("node \[[italic]" + node['id'] +"[/italic]] awaiting confirmation")
        table = Table(show_header=False, show_footer=False, show_lines=False, show_edge=False)
        table.add_column(width=(self.__first_column_width+1), justify="right")
        table.add_column()
        table.add_row('[bright_magenta]Node[/]',f"[cyan]{node['id']}[/]")
        table.add_row('[bright_magenta]Reference[/]',node.get('reference', '-'))
        table.add_row('[bright_magenta]Description[/]',node.get('description', '-'))
        self.__console.print(table)

        while y_or_n.lower() not in ['y', 'n']:
            table = Table(show_header=False, show_footer=False, show_lines=False, show_edge=False)
            table.add_column(width=self.__first_column_width)
            table.add_column(justify="right")
            table.add_column()
            self.__console.print(table)
            self.__console.line()
            y_or_n = Prompt.ask("[white] Do you want to run the node \[{}]? [green]y[/](yes) / [bright_red]n[/](no/skip)".format(node['id']),
                                console=self.__console,
                                show_choices=False,
                                choices=["n","y"])

        self.__console.line()
        self.__console.rule()

        if y_or_n.strip().lower() == 'y':
            self.api_client.approve_node(node['id'])
        elif y_or_n.strip().lower() == 'n':
            self.api_client.disapprove_node(node['id'])

        self.approved_nodes.add(node['id'])
        return True

    def handle_checkpoint_node(self, node):
        y_or_n = ''
        self.__console.line()
        self.__console.rule(f"Checkpoint Reached: [italic]{node['id']}[/italic]")

        description = node.get('description', 'Do you want to proceed?')
        if node.get('reference'):
            description += f"\n[dim]Reference: {node.get('reference')}[/dim]"

        self.__console.print(description, justify="center")

        while y_or_n.lower() not in ['y', 'n']:
            y_or_n = Prompt.ask("[white]Do you want to continue? [green]y[/](yes) / [bright_red]n[/](no/skip)",
                                console=self.__console,
                                show_choices=False,
                                choices=["n","y"])

        self.__console.line()
        self.__console.rule()

        if y_or_n.strip().lower() == 'y':
            self.api_client.approve_node(node['id'])
        elif y_or_n.strip().lower() == 'n':
            self.api_client.disapprove_node(node['id'])

        self.approved_nodes.add(node['id'])

    def print_node_status_change(self, node):
        node_type = node.get('type')
        status = node.get('status')
        timestamp = node.get('ended', '')

        if not timestamp:
             timestamp = datetime.now().strftime('%H:%M:%S')

        table = Table(show_header=False, show_footer=False, show_lines=False, show_edge=False)
        table.add_column(width=(self.__first_column_width +1), justify="right")
        table.add_column()

        message = ""
        if node_type == 'info' and status == NodeStatus.ENDED.value:
            message = f"[bold cyan]INFO:[/] [cyan]{node.get('description', node['id'])}[/]"
        else:
            status_text = self._render_status(node['status'])
            message = f"Node [cyan]{node['id']}[/] is {status_text}"

        table.add_row(timestamp, message)
        self.__console.print(table)

    def handle_retry(self, node):
        y_or_n = ''
        self.__console.line()
        self.__console.rule("node \[[italic]" + node['id'] +"[/italic]] failed")
        table = Table(show_header=False, show_footer=False, show_lines=False, show_edge=False)
        #table.add_column()
        table.add_column(width=(self.__first_column_width+1), justify="right")
        table.add_column()
        table.add_row('[bright_magenta]Node[/]',f"[cyan]{node['id']}[/]")
        table.add_row('[bright_magenta]Reference[/]',node.get('reference', '-'))
        table.add_row('[bright_magenta]Description[/]',node.get('description', '-'))
        self.__console.print(table)

        while y_or_n.lower() not in ['y', 'n', 's', 'l']:
            table = Table(show_header=False, show_footer=False, show_lines=False, show_edge=False)
            table.add_column(width=self.__first_column_width)
            table.add_column(justify="right")
            table.add_column()
            self.__console.print(table)
            self.__console.line()
            y_or_n = Prompt.ask("[white] Do you want to restart the node \[{}]? [green]y[/](yes) / [bright_red]n[/](no) / [cyan]s[/](skip) / [bright_magenta]l[/](logs)".format(node['id']),
                                console=self.__console,
                                show_choices=False,
                                choices=["n","y","s","l"])

            if y_or_n == 'l':
                stdout = self.api_client.get_node_stdout(node['id'])
                if stdout:
                    self.__console.line()
                    self.__console.print(Text.from_ansi(stdout))
        self.__console.line()
        self.__console.rule()

        if y_or_n == 'y':
            self.api_client.restart_node(node['id'])
        elif y_or_n == 's':
            self.api_client.skip_node(node['id'])
        elif y_or_n == 'n':
            self.declined_retry_nodes.add(node['id'])

    def _request_stop(self):
        self.stop_requested = True

    def _handle_stop_request(self):
        self.api_client.pause_workflow()
        with self.console_lock:
            self.__console.print("\n")
            self.__console.print("[bold yellow]Stop workflow requested.[/]")
            self.__console.print("[bold yellow]Choose stop mode[/]: \\[g][dark_orange]raceful[/], \\[h][red]ard[/], or \\[c][cyan]ancel[/]?")
            choice = self.__console.input("> ")
            self._logger.info(f"User chose: {choice}")
            if choice.lower() == 'g':
                self.api_client.stop_workflow(mode="graceful")
                self.__console.print("[yellow]Graceful stop requested.[/]")
            elif choice.lower() == 'h':
                self.api_client.stop_workflow(mode="hard")
                self.__console.print("[red]Hard stop requested.[/]")
            else:
                self.api_client.resume_workflow()
                self.__console.print("[green]Stop request canceled.[/]")
        self.stop_requested = False

    def run(self):
        self._logger.info("WorkflowOutput run")
        self.draw_init()

        is_tty = sys.stdin.isatty()
        if is_tty:
            old_settings = termios.tcgetattr(sys.stdin)

        try:
            if is_tty:
                tty.setcbreak(sys.stdin.fileno())

            status_data = None
            while not self.event.is_set():
                if is_tty and select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], []):
                    c = sys.stdin.read(1)
                    if c == '\x18': # Ctrl+X
                        self._request_stop()

                if self.stop_requested:
                    self._handle_stop_request()

                status_data = self.api_client.get_workflow_status()
                status = status_data.get('status') if status_data else None
                self._logger.info(f"Checking status: {status}")

                if status == "ended":
                    break

                if status == "failed" and not self._WorkflowOutput__interactive_retry:
                    break

                self.draw_step()

                if hasattr(self, 'user_chose_to_quit') and self.user_chose_to_quit:
                    break

                self.draw_pause()
        finally:
            if is_tty:
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)


        if not self.event.is_set():
            self._logger.info(f"Final status: {status}. Exiting loop.")
            self.draw_end(status_data=status_data)
