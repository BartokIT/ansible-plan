from enum import Enum
import threading
import os
import logging
import abc
import time
from typing import Callable
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.text import Text
import sys
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static, Tree, RichLog, Rule, DataTable
from textual.containers import Container, Horizontal, Vertical
from textual import work
from textual.reactive import reactive
import itertools
import networkx as nx

from .api_client import ApiClient

# These enums are needed for status comparison.
# Ideally, they would be shared between frontend and backend.
class WorkflowStatus(Enum):
    NOT_STARTED = 'not_started'
    RUNNING = 'running'
    ENDED = 'ended'
    FAILED = 'failed'

class NodeStatus(Enum):
    RUNNING = 'running'
    PRE_RUNNING = 'pre_running'
    ENDED = 'ended'
    FAILED = 'failed'
    SKIPPED = 'skipped'
    NOT_STARTED = 'not_started'


class WorkflowOutput(threading.Thread):
    '''
    A general workflow output class to be implemented by subclasses
    '''
    def __init__(self, backend_url, event, logging_dir, log_level, cmd_args):
        threading.Thread.__init__(self)
        self._define_logger(logging_dir, log_level)
        self.api_client = ApiClient(backend_url)
        self._refresh_interval = 2
        self.__verify_only = cmd_args.verify_only
        self.event: threading.Event = event

    def is_verify_only(self):
        return self.__verify_only

    def _define_logger(self, logging_dir, level):
        logger_name = self.__class__.__name__
        # Use a fixed log name for now, as we don't have the workflow object here.
        self._log_name = "frontend.log"
        logger_file_path = os.path.join(logging_dir, self._log_name)
        if not os.path.exists(os.path.dirname(logger_file_path)):
            os.makedirs(os.path.dirname(logger_file_path))

        logger = logging.getLogger(logger_name)
        if level:
            logger.setLevel(getattr(logging, level.upper()))
        logger_handler = logging.handlers.TimedRotatingFileHandler(
            logger_file_path,
            when='d',
            backupCount=3,
            encoding='utf8'
        )
        logger_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
        logger.addHandler(logger_handler)
        self._logger = logger
        self._logging_dir = logging_dir

    def run(self):
        self._logger.info("WorkflowOutput run")
        self.draw_init()
        status = self.api_client.get_workflow_status()
        while status not in [WorkflowStatus.ENDED.value, WorkflowStatus.FAILED.value]:
            self._logger.info(f"Checking status: {status}")
            self.draw_step()
            self.draw_pause()
            status = self.api_client.get_workflow_status()
        self._logger.info(f"Final status: {status}. Exiting loop.")
        self.draw_end()

    @abc.abstractmethod
    def draw_init(self, *args, **kwargs):
        ''' Draw initialization'''
        pass

    @abc.abstractmethod
    def draw_end(self, *args, **kwargs):
        ''' Draw initialization'''
        pass

    @abc.abstractmethod
    def draw_step(self):
        ''' Draw the workflow'''
        pass

    @abc.abstractmethod
    def draw_pause(self):
        ''' Need to be implemented to pause after a draw step'''
        pass


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

    def draw_end(self):
        nodes = self.api_client.get_all_nodes()
        table = Table(title="Running recap")

        table.add_column("Node", justify="left", style="cyan", no_wrap=True)
        table.add_column("Playbook", style="bright_magenta")
        table.add_column("Ref.", style="cyan")
        table.add_column("Status")

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
                self.__console.print(stdout)


        if y_or_n == 'y':
            self.api_client.restart_node(node['id'])
        elif y_or_n == 's':
            self.api_client.skip_node(node['id'])


class TextualWorkflowOutput(WorkflowOutput):
    _log_name = 'textual.log'

    def __init__(self, backend_url, event, logging_dir, log_level, cmd_args):
        # We don't call super().__init__ because Textual has its own way of running.
        self._define_logger(logging_dir, log_level)
        self.api_client = ApiClient(backend_url)
        self.app = self.WorkflowApp(self)

    def run(self):
        """
        This method is called directly from __main__.py for textual mode.
        It launches the Textual app.
        """
        self.app.run()

    # The following methods are not used in Textual mode as the app handles the loop.
    def draw_init(self): pass
    def draw_end(self): pass
    def draw_step(self): pass
    def draw_pause(self): pass


    class WorkflowApp(App):
        CSS = """
        .sidebar {
            width: 40;
            height: 100%;
            dock: left;
        }
        #playbook_stdout {
            background: $surface;
        }
        """

        def __init__(self, outer_instance):
            super().__init__()
            self.outer_instance = outer_instance
            self.api_client = outer_instance.api_client
            self.tree_nodes = {}
            self.node_data = {}
            self.graph = nx.DiGraph()
            self.spinner_icons = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
            self.status_icons = {
                NodeStatus.NOT_STARTED.value: "○",
                NodeStatus.PRE_RUNNING.value: "[yellow]…[/yellow]",
                NodeStatus.RUNNING.value: "[yellow]○[/yellow]",
                NodeStatus.ENDED.value: "[green]✔[/green]",
                NodeStatus.FAILED.value: "[red]✖[/red]",
                NodeStatus.SKIPPED.value: "[cyan]»[/cyan]",
            }
            self.node_spinners = {}
            self.stdout_watcher = None

        def compose(self) -> ComposeResult:
            yield Header()
            with Horizontal():
                yield Tree("Workflow", id="workflow_tree", classes="sidebar")
                with Vertical():
                    yield DataTable(id="node_details", show_cursor=False, show_header=False)
                    yield Rule()
                    yield RichLog(id="playbook_stdout", markup=True)
            yield Footer()

        def on_mount(self) -> None:
            self.initial_setup()
            self.set_interval(2, self.update_node_statuses)

        @work(thread=True)
        def initial_setup(self):
            # Fetch graph and node data once
            edges = self.api_client.get_workflow_graph()
            self.graph.add_edges_from(edges)

            nodes = self.api_client.get_all_nodes()
            for node in nodes:
                self.node_data[node['id']] = node

            # Build the tree
            tree = self.query_one(Tree)
            tree.clear()
            root_node_id = "_root"
            root_node = tree.root
            root_node.data = root_node_id
            root_node.set_label("Workflow")
            self.tree_nodes[root_node_id] = root_node

            self._build_tree(root_node_id, root_node)
            tree.root.expand_all()

        def _build_tree(self, node_id, tree_node):
            for child_id in self.graph.successors(node_id):
                if child_id in ['_s', '_e']:
                    continue

                child_node_data = self.node_data.get(child_id, {})
                node_type = child_node_data.get('type')

                allow_expand = node_type == 'block'
                if node_type == 'block':
                    label = f"[b]{child_id}[/b]"
                else:
                    icon = self.status_icons.get(child_node_data.get('status'), " ")
                    label = f"{icon} {child_id}"

                child_tree_node = tree_node.add(label, data=child_id, allow_expand=allow_expand)
                self.tree_nodes[child_id] = child_tree_node

                if self.graph.out_degree(child_id) > 0:
                    self._build_tree(child_id, child_tree_node)

        @work(thread=True)
        def update_node_statuses(self):
            nodes = self.api_client.get_all_nodes()
            for node in nodes:
                node_id = node['id']
                if node_id in self.tree_nodes:
                    # Update internal data store
                    self.node_data[node_id] = node

                    tree_node = self.tree_nodes[node_id]
                    status = node['status']

                    if status == NodeStatus.RUNNING.value:
                        if node_id not in self.node_spinners:
                            self.node_spinners[node_id] = self.update_spinner(tree_node, node)
                    else:
                        if node_id in self.node_spinners:
                            self.node_spinners[node_id].cancel()
                            del self.node_spinners[node_id]
                        else:
                            # This node was not running, so no spinner to cancel.
                            # We need to set its label here.
                            if node.get('type') == 'block':
                                label = f"[b]{node_id}[/b]"
                            else:
                                icon = self.status_icons.get(status, " ")
                                label = f"{icon} {node_id}"
                            tree_node.set_label(label)

        def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
            if self.stdout_watcher:
                self.stdout_watcher.cancel()
                self.stdout_watcher = None

            node_id = event.node.data
            node_data = self.node_data.get(node_id)

            if not node_data:
                return

            details_table = self.query_one("#node_details", DataTable)
            details_table.clear()
            details_table.add_column("Property", width=20)
            details_table.add_column("Value")

            def add_detail(key, value):
                height = str(value).count('\n') + 1
                details_table.add_row(key, value, height=height)

            add_detail("ID", node_data.get('id'))
            if node_data.get('type') == 'playbook':
                add_detail("Playbook", node_data.get('playbook', 'N/A'))
                add_detail("Description", node_data.get('description', 'N/A'))
                add_detail("Reference", node_data.get('reference', 'N/A'))
                self.show_stdout(node_id)
                if node_data['status'] == NodeStatus.RUNNING.value:
                    self.stdout_watcher = self.watch_stdout(node_id)
            elif node_data.get('type') == 'block':
                 add_detail("Type", "Block")


        @work(thread=True)
        def update_spinner(self, tree_node, node_data):
            spinner_cycle = itertools.cycle(self.spinner_icons)
            node_id = node_data['id']
            try:
                while True: # We will cancel this worker externally
                    icon_char = next(spinner_cycle)
                    icon = f"[yellow]{icon_char}[/yellow]"
                    if node_data.get('type') == 'block':
                        label = f"{icon} [b]{node_data['id']}[/b]"
                    else:
                        label = f"{icon} {node_data['id']}"
                    tree_node.set_label(label)
                    time.sleep(0.1)
            finally:
                # This block will run when the worker is cancelled.
                # We need the FINAL status here.
                final_node_data = self.node_data.get(node_id, {})
                status = final_node_data.get('status')

                if final_node_data.get('type') == 'block':
                    label = f"[b]{node_id}[/b]"
                else:
                    icon = self.status_icons.get(status, " ")
                    label = f"{icon} {node_id}"
                tree_node.set_label(label)

        @work(exclusive=True, thread=True)
        def show_stdout(self, node_id: str):
            """Reads and displays the entire stdout for a given node."""
            stdout_log = self.query_one("#playbook_stdout", RichLog)
            stdout_log.display = False
            stdout_log.display = True
            stdout_log.clear()

            stdout = self.api_client.get_node_stdout(node_id)
            stdout_log.write(stdout)

        @work(exclusive=True, thread=True)
        def watch_stdout(self, node_id: str):
            stdout_log = self.query_one("#playbook_stdout", RichLog)
            stdout_log.clear()

            last_content = ""
            while True: # Will be cancelled
                current_stdout = self.api_client.get_node_stdout(node_id)
                if current_stdout != last_content:
                    new_content = current_stdout[len(last_content):]
                    stdout_log.write(new_content)
                    last_content = current_stdout

                status_response = self.api_client.get_all_nodes()
                node_status = next((n['status'] for n in status_response if n['id'] == node_id), None)
                if node_status != NodeStatus.RUNNING.value:
                    break

                time.sleep(0.5)
