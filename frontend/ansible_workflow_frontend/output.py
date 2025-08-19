from enum import Enum
import threading
import os
import logging
import logging.handlers
import abc
import time
from typing import Callable
from datetime import datetime
from rich.console import Console
from rich.highlighter import Highlighter
from rich.table import Table
from rich.text import Text
import sys
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static, Tree, RichLog, Rule, DataTable, Button
from textual.containers import Container, Horizontal, Vertical
from textual.message import Message
from textual import work
from textual.reactive import reactive
from textual.css.query import NoMatches
import itertools
import networkx as nx
import httpx

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

    def draw_end(self):
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


class NullHighlighter(Highlighter):
    def highlight(self, text):
        pass

class TextualWorkflowOutput(WorkflowOutput):
    _log_name = 'textual.log'

    def __init__(self, backend_url, event, logging_dir, log_level, cmd_args):
        # We don't call super().__init__ because Textual has its own way of running.
        self._define_logger(logging_dir, log_level)
        self.api_client = ApiClient(backend_url)
        self.cmd_args = cmd_args
        self.app = self.WorkflowApp(self, cmd_args)

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
        CSS_PATH = "style.css"

        status_message = reactive("Connecting to backend...")

        def __init__(self, outer_instance, cmd_args):
            super().__init__()
            self.outer_instance = outer_instance
            self.workflow_filename = os.path.basename(cmd_args.workflow)
            if self.outer_instance.is_verify_only():
                self.title = f"Workflow Viewer (Verify Only)"
            else:
                self.title = "Workflow Viewer"
            self.theme = "gruvbox"
            self.api_client = outer_instance.api_client
            self.selected_node_id = None
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
            # This dictionary now only serves as a flag to indicate if a spinner worker
            # has been started for a node, to prevent duplicates.
            self.active_spinners = set()
            self.stdout_watcher = None
            self._shutdown_event = threading.Event()

        def compose(self) -> ComposeResult:
            yield Header()
            with Horizontal():
                yield Tree(self.workflow_filename, id="workflow_tree", classes="sidebar")
                with Vertical():
                    yield DataTable(id="node_details", show_cursor=False, show_header=False)
                    with Horizontal(id="action_buttons"):
                        yield Button("Relaunch", id="relaunch_button", variant="success")
                        yield Button("Skip", id="skip_button", variant="error")
                    playbook_stdout_log = RichLog(id="playbook_stdout", markup=False, highlight=True)
                    playbook_stdout_log.highlighter = NullHighlighter()
                    yield playbook_stdout_log
            yield Static("Connecting to backend...", id="status_bar")
            yield Footer()

        def watch_status_message(self, message: str) -> None:
            try:
                status_bar = self.query_one("#status_bar", Static)
                status_bar.update(message)
            except NoMatches:
                pass

        def update_health_status(self) -> None:
            action_buttons = self.query_one("#action_buttons")
            if self.api_client.check_health():
                self.status_message = "[green]Backend: Connected[/green]"
            else:
                self.status_message = "[red]Backend: Disconnected[/red]"
                action_buttons.display = False

        def on_mount(self) -> None:
            self.initial_setup()
            self.set_interval(5, self.update_health_status)
            self.set_interval(0.5, self.update_node_statuses)

        def action_quit(self) -> None:
            """Called when the user quits the application."""
            self._shutdown_event.set()
            self.exit()

        @work(thread=True)
        def initial_setup(self):
            # Fetch graph and node data once
            edges = self.api_client.get_workflow_graph()
            if edges is not None:
                self.graph.add_edges_from(edges)
            if "_root" not in self.graph:
                self.graph.add_node("_root")

            nodes = self.api_client.get_all_nodes()
            if nodes is not None:
                for node in nodes:
                    self.node_data[node['id']] = node

            # Build the tree
            tree = self.query_one(Tree)
            root_node_id = "_root"
            root_node = tree.root
            root_node.data = root_node_id
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

        @work(thread=True, exclusive=True)
        def update_node_statuses(self):
            # Sanitize the data from the API to prevent processing duplicate statuses
            nodes_from_api = self.api_client.get_all_nodes()
            if nodes_from_api is None:
                return
            final_node_states = {node['id']: node for node in nodes_from_api}

            for node_id, node in final_node_states.items():
                if node_id in self.tree_nodes and node_id != "_root":
                    # Update the central data store
                    self.node_data[node_id] = node

                    tree_node = self.tree_nodes[node_id]
                    status = node['status']

                    if status == NodeStatus.RUNNING.value:
                        # If a spinner isn't already running for this node, start one.
                        if node_id not in self.active_spinners:
                            self.active_spinners.add(node_id)
                            self.update_spinner(tree_node, node)
                    else:
                        # For any non-running state, we are the source of truth.
                        # The spinner, if it exists, will see the state change and stop itself.
                        # We just set the final label.
                        if node.get('type') == 'block':
                            label = f"[b]{node_id}[/b]"
                        else:
                            icon = self.status_icons.get(status, " ")
                            label = f"{icon} {node_id}"
                        tree_node.set_label(label)

                    # If the updated node is the one currently selected, refresh the action buttons
                    if node_id == self.selected_node_id:
                        action_buttons = self.query_one("#action_buttons")
                        if status == NodeStatus.FAILED.value:
                            action_buttons.display = True
                        else:
                            action_buttons.display = False

        def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
            if self.stdout_watcher:
                self.stdout_watcher.cancel()
                self.stdout_watcher = None

            node_id = event.node.data
            self.selected_node_id = node_id
            node_data = self.node_data.get(node_id)
            action_buttons = self.query_one("#action_buttons")

            if not node_data:
                action_buttons.display = False
                return

            details_table = self.query_one("#node_details", DataTable)
            details_table.clear()
            if not details_table.columns:
                details_table.add_column("Property", width=20)
                details_table.add_column("Value")

            def add_detail(key, value):
                details_table.add_row(key, value, height=None)

            add_detail("ID", node_data.get('id'))
            if node_data.get('type') == 'playbook':
                add_detail("Playbook", node_data.get('playbook', 'N/A'))
                add_detail("Description", node_data.get('description', 'N/A'))
                add_detail("Reference", node_data.get('reference', 'N/A'))
                add_detail("Started", node_data.get('started', 'N/A'))
                add_detail("Ended", node_data.get('ended', 'N/A'))
                self.show_stdout(node_id)
                if node_data['status'] == NodeStatus.RUNNING.value:
                    self.stdout_watcher = self.watch_stdout(node_id)
            elif node_data.get('type') == 'block':
                 add_detail("Type", "Block")

            if node_data.get('status') == NodeStatus.FAILED.value:
                action_buttons.display = True
            else:
                action_buttons.display = False

        def on_button_pressed(self, event: Button.Pressed) -> None:
            """Called when a button is pressed."""
            if self.selected_node_id:
                if event.button.id == "relaunch_button":
                    self.api_client.restart_node(self.selected_node_id)
                    # Clear the log and start watching for new output
                    self.query_one("#playbook_stdout", RichLog).clear()
                    if self.stdout_watcher:
                        self.stdout_watcher.cancel()
                    self.stdout_watcher = self.watch_stdout(self.selected_node_id)
                elif event.button.id == "skip_button":
                    self.api_client.skip_node(self.selected_node_id)

            # Hide buttons after action
            self.query_one("#action_buttons").display = False

        @work(exclusive=True, thread=True)
        def watch_stdout(self, node_id: str):
            stdout_log = self.query_one("#playbook_stdout", RichLog)
            last_content = self.api_client.get_node_stdout(node_id)
            if last_content is None:
                return

            while not self._shutdown_event.is_set():
                time.sleep(0.5)
                current_stdout = self.api_client.get_node_stdout(node_id)
                if current_stdout is None:
                    break
                if current_stdout != last_content:
                    new_content = current_stdout[len(last_content):]
                    text = Text.from_ansi(new_content)
                    stdout_log.write(text)
                    last_content = current_stdout

                status_response = self.api_client.get_all_nodes()
                if status_response is None:
                    break
                node_status = next((n['status'] for n in status_response if n['id'] == node_id), None)
                if node_status != NodeStatus.RUNNING.value:
                    break

        @work(thread=True)
        def update_spinner(self, tree_node, node_data):
            """
            This worker is now self-terminating. It spins as long as the node's
            status is 'running' in the central self.node_data store.
            """
            spinner_cycle = itertools.cycle(self.spinner_icons)
            node_id = node_data['id']

            while self.node_data.get(node_id, {}).get('status') == NodeStatus.RUNNING.value and not self._shutdown_event.is_set():
                icon_char = next(spinner_cycle)
                icon = f"[yellow]{icon_char}[/yellow]"

                # Use the original node_data for static info like type and id
                if node_data.get('type') == 'block':
                    label = f"{icon} [b]{node_id}[/b]"
                else:
                    label = f"{icon} {node_id}"

                # Final check to prevent a race condition where the status changes
                # between the while-check and this set_label call.
                if self.node_data.get(node_id, {}).get('status') == NodeStatus.RUNNING.value:
                    tree_node.set_label(label)

                time.sleep(0.1)

            # The loop has ended, meaning the node is no longer running.
            # The main update_node_statuses loop is now responsible for setting the
            # final label. This worker just needs to clean up its flag.
            if node_id in self.active_spinners:
                self.active_spinners.remove(node_id)

        @work(exclusive=True, thread=True)
        def show_stdout(self, node_id: str):
            """Reads and displays the entire stdout for a given node."""
            stdout_log = self.query_one("#playbook_stdout", RichLog)
            stdout_log.clear()
            stdout = self.api_client.get_node_stdout(node_id)
            if stdout is not None:
                text = Text.from_ansi(stdout)
                stdout_log.write(text)
