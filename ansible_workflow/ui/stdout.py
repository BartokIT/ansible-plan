import time
from datetime import datetime
from rich.console import Console
from rich.table import Table
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

    def draw_init(self):
        self._logger.debug("Initializing stdout output")
        if self.is_verify_only():
            self.__console.print("[bold yellow]Running in VERIFY ONLY mode[/]", justify="center")
        self.__console.print("[italic]Waiting for workflow to start...[/]", justify="center")

        nodes = self.api_client.get_all_nodes()
        while not nodes:
            time.sleep(1)
            nodes = self.api_client.get_all_nodes()

        table = Table(title="Workflow nodes")
        table.add_column("Node", justify="left", style="cyan", no_wrap=True)
        table.add_column("Playbook", style="bright_magenta")
        table.add_column("Ref.", style="cyan")
        table.add_column("Status")

        if nodes:
            for node in nodes:
                if node['type'] == 'playbook':
                    self.known_nodes[node['id']] = node
                    table.add_row(
                        node['id'],
                        node.get('playbook', 'N/A'),
                        node.get('reference', 'N/A'),
                        self._render_status(node['status'])
                    )
        self.__console.print(table)
        self.__console.print("")
        self.__console.print("[italic]Running[/] ...", justify="center")

    def draw_step(self):
        nodes = self.api_client.get_all_nodes()
        if nodes:
            for node in nodes:
                node_id = node['id']
                if node_id in self.known_nodes and self.known_nodes[node_id]['status'] != node['status']:
                    self.print_node_status_change(node)
                    self.known_nodes[node_id] = node

                    if node['status'] == NodeStatus.FAILED.value and self.__interactive_retry:
                        self.handle_retry(node)


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

        table.add_column("Node", justify="left", style="cyan", no_wrap=True)
        table.add_column("Playbook", style="bright_magenta")
        table.add_column("Ref.", style="cyan")
        table.add_column("Status")

        if nodes:
            for node in nodes:
                if node['type'] == 'playbook':
                    table.add_row(
                        node['id'],
                        node.get('playbook', 'N/A'),
                        node.get('reference', 'N/A'),
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
        else:
            return 'unknown'

    def print_node_status_change(self, node):
        table = Table(show_header=False, show_footer=False, show_lines=False, show_edge=False)
        table.add_column()
        table.add_column()
        status_text = self._render_status(node['status'])
        table.add_row(
            datetime.now().strftime('%H:%M:%S'),
            f"Node [cyan]{node['id']}[/] is {status_text}"
        )
        self.__console.print(table)

    def handle_retry(self, node):
        y_or_n = ''

        table = Table(show_header=False, show_footer=False, show_lines=False, show_edge=False)
        table.add_column()
        table.add_column(justify="right")
        table.add_column()

        table.add_row('','','')
        table.add_section()
        table.add_row('','[bright_magenta]Node[/]',f"[cyan]{node['id']}[/]")
        table.add_row('','[bright_magenta]Reference[/]',node.get('reference', 'N/A'))
        table.add_row('','     [bright_magenta]Description[/]',node.get('description', 'N/A'))
        table.add_section()
        table.add_row('','','')
        self.__console.print(table)

        while y_or_n.lower() not in ['y', 'n', 's', 'l']:
            table = Table(show_header=False, show_footer=False, show_lines=False, show_edge=False)
            table.add_column()
            table.add_column(justify="right")
            table.add_column()
            table.add_row('', '?               ', '[white]-> Do you want to restart? [green]y[/](yes) / [bright_red]n[/](no) / [cyan]s[/](skip) / [bright_magenta]l[/](logs): ')
            self.__console.print(table)
            y_or_n = self.__console.input('  │ >>>>>>>>>>>>>>>> │ ')
            if y_or_n == 'l':
                stdout = self.api_client.get_node_stdout(node['id'])
                if stdout:
                    self.__console.print(stdout)


        if y_or_n == 'y':
            self.api_client.restart_node(node['id'])
        elif y_or_n == 's':
            self.api_client.skip_node(node['id'])
