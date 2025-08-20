import os
import time
import threading
import itertools
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", message="networkx backend defined more than once: nx-loopback")
    import networkx as nx
from rich.highlighter import Highlighter
from rich.text import Text
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static, Tree, RichLog, DataTable, Button
from textual.containers import Horizontal, Vertical
from textual import work
from textual.reactive import reactive
from textual.css.query import NoMatches
from .base import WorkflowOutput
from ..core.models import NodeStatus
from .api_client import ApiClient


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
        self._WorkflowOutput__verify_only = cmd_args.verify_only
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
