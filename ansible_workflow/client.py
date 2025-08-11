import Pyro5.api
from itertools import cycle
from textual.app import App, ComposeResult
from textual.widgets import Header, Tree, Log, Button, Static
from textual.containers import Horizontal, Vertical
from textual import work
import sys
import os
import asyncio
from rich.pretty import Pretty

# The name of the Pyro object registered on the name server
PYRO_CONTROLLER_NAME = "ansible.workflow.controller"


class NodeDetails(Static):
    """A widget to display details of the selected node."""
    def update_details(self, details: dict):
        if not details:
            self.update("Select a node to see details.")
            return

        node_type = details.get('type')
        if node_type == 'PNode':
            # Display playbook details
            text = (
                f"[bold]Playbook Node:[/bold] {details.get('details', {}).get('playbook', 'N/A')}\n"
                f"[bold]Status:[/bold] {details.get('status', 'N/A')}\n"
                f"[bold]Inventory:[/bold] {details.get('details', {}).get('inventory', 'N/A')}\n"
                f"[bold]Started:[/bold] {details.get('started', 'N/A')}\n"
                f"[bold]Ended:[/bold] {details.get('ended', 'N/A')}\n\n"
                f"[bold]Extra Vars:[/bold]\n{Pretty(details.get('details', {}).get('extravars', {}))}"
            )
            self.update(text)
        elif node_type == 'BNode':
            # Display block details
            text = (
                f"[bold]Block Node[/bold]\n"
                f"[bold]Strategy:[/bold] {details.get('details', {}).get('strategy', 'N/A')}"
            )
            self.update(text)
        else:
            self.update("Select a node to see details.")


class WorkflowUi(App):
    """A Textual app to manage and monitor Ansible workflows."""

    CSS_PATH = "client.css"

    BINDINGS = [
        ("d", "toggle_dark", "Toggle dark mode"),
        ("q", "quit", "Quit"),
    ]

    def __init__(self, controller_proxy, workflow_path, inventory_path):
        super().__init__()
        self.controller = controller_proxy
        self.workflow_path = workflow_path
        self.inventory_path = inventory_path
        self.polling_timer = None
        self.spinner_cycle = cycle(["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
        self.log_stream_worker = None

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        with Horizontal(id="header-container"):
            yield Button("Restart", id="restart-button", variant="primary")
            yield Button("Stop", id="stop-button", variant="error")

        with Horizontal(id="main-container"):
            with Vertical(id="tree-container"):
                yield Tree("Workflow", id="workflow-tree", data={'id': 'root', 'label': 'Workflow'})
            with Vertical(id="right-pane"):
                with Vertical(id="details-container"):
                    yield NodeDetails("Select a node to see details.")
                with Vertical(id="log-container"):
                    yield Log(id="output-log", auto_scroll=True)
        yield Static(id="status-bar", classes="box")

    def on_mount(self) -> None:
        """Called when app is mounted."""
        self.query_one("#status-bar").update("Loading workflow...")
        self.load_workflow_and_build_tree()
        self.polling_timer = self.set_interval(1, self.update_statuses)

    @work(exclusive=True)
    async def load_workflow_and_build_tree(self) -> None:
        """Loads the workflow on the server and builds the initial tree."""
        status_bar = self.query_one("#status-bar")
        try:
            result = self.controller.load_workflow(self.workflow_path, self.inventory_path)
            status_bar.update(result)
            if "Error" in result and "Reconnected" not in result:
                return

            tree = self.query_one(Tree)
            tree.clear()
            workflow_data = self.controller.get_input_data()
            if not workflow_data:
                status_bar.update("Error: Failed to get workflow data from server.")
                return
            actual_workflow = workflow_data[1:-1] # Skip 's' and 'e' nodes
            self._recursive_build_tree(tree.root, actual_workflow)
            tree.root.expand_all() # Expand all nodes as requested
            status_bar.update("Workflow tree built. Triggering run...")
            self.run_workflow()

        except Exception as e:
            status_bar.update(f"Error: {e}")

    @work(exclusive=True)
    async def run_workflow(self) -> None:
        """Runs the workflow on the server."""
        status_bar = self.query_one("#status-bar")
        try:
            # Short delay to allow UI to settle before starting
            await asyncio.sleep(0.5)
            result = self.controller.run()
            status_bar.update(f"Server: {result}")
        except Exception as e:
            status_bar.update(f"Error starting workflow: {e}")

    def _recursive_build_tree(self, parent_node, data):
        for item in data:
            node_id = item['id']
            if 'block' in item:
                label = item.get('name', f"Block: {node_id}")
                new_node = parent_node.add(label, data={'id': node_id, 'label': label, 'type': 'BNode'})
                self._recursive_build_tree(new_node, item['block'])
            else:
                playbook_name = os.path.basename(item.get('import_playbook', 'Unknown Playbook'))
                label = item.get('name', playbook_name)
                new_node = parent_node.add_leaf(label, data={'id': node_id, 'label': label, 'type': 'PNode'})

    @work(exclusive=False)
    async def update_statuses(self) -> None:
        """Polls the server for status updates and refreshes the tree."""
        try:
            statuses = self.controller.get_nodes_status()
            tree = self.query_one(Tree)

            status_map = {
                'not_started': "\\[ ]",
                'running': f"\\[[yellow]{next(self.spinner_cycle)}[/yellow]]",
                'ended': "\\[[green]✔[/green]]",
                'failed': "\\[[red]✖[/red]]",
                'stopped': "\\[[orange]S[/orange]]",
                'skipped': "\\[[grey]-[/grey]]",
            }

            def update_node_label(node):
                if not hasattr(node, 'data') or node.data is None or 'id' not in node.data:
                    return

                node_id = node.data.get('id')
                base_label = node.data.get('label')

                if node_id and base_label and node_id in statuses:
                    node_info = statuses[node_id]
                    status = node_info['status']
                    node_type = node_info['type']

                    if node_type == 'BNode':
                        new_label = base_label
                    else:
                        icon = status_map.get(status, '❔')
                        new_label = f"{icon} {base_label}"

                    if node.label != new_label:
                        node.set_label(new_label)

                for child_node in node.children:
                    update_node_label(child_node)

            update_node_label(tree.root)

        except Exception:
            # Polling might fail if server shuts down, just stop the timer
            if self.polling_timer:
                self.polling_timer.stop()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button press events."""
        status_bar = self.query_one("#status-bar")
        if event.button.id == "stop-button":
            try:
                result = self.controller.stop()
                status_bar.update(f"Server: {result}")
            except Exception as e:
                status_bar.update(f"Error stopping workflow: {e}")
        elif event.button.id == "restart-button":
            try:
                result = self.controller.restart_workflow()
                status_bar.update(f"Server: {result}")
                # After restarting, clear the log and details and reload the workflow
                self.query_one("#output-log").clear()
                self.query_one(NodeDetails).update_details({})
                self.load_workflow_and_build_tree()
            except Exception as e:
                status_bar.update(f"Error restarting workflow: {e}")

    async def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Handle tree node selection and start tailing the log."""
        if self.log_stream_worker:
            self.log_stream_worker.cancel()

        node_id = event.node.data.get('id')
        if not node_id or node_id == 'root':
            self.query_one(NodeDetails).update_details({})
            return

        # Update details panel
        details = self.controller.get_node_details(node_id)
        self.query_one(NodeDetails).update_details(details)

        # Update log panel
        log_widget = self.query_one("#output-log")
        log_widget.clear()

        if event.node.data.get('type') == 'PNode':
            log_widget.write(f"--- Tailing output for {event.node.label} ---")
            self.log_stream_worker = self.stream_log(node_id)
        else: # BNode
            log_widget.write("No output for Block nodes.")

    async def action_quit(self) -> None:
        """Custom quit action to notify the server."""
        try:
            self.controller.request_shutdown()
        except Pyro5.errors.CommunicationError:
            pass
        self.exit()

    @work(exclusive=False)
    async def stream_log(self, node_id: str) -> None:
        """Worker to stream log output for a node."""
        log_widget = self.query_one("#output-log")
        offset = 0
        while self.is_running:
            try:
                new_content, new_offset = self.controller.tail_playbook_output(node_id, offset)
                if new_content:
                    log_widget.write(new_content)
                offset = new_offset
                await asyncio.sleep(1)
            except Exception:
                # Log might have been cancelled, or server shut down
                break


def main(workflow_path, inventory_path):
    """Main function to run the TUI client."""
    try:
        controller_proxy = Pyro5.api.Proxy(f"PYRONAME:{PYRO_CONTROLLER_NAME}")
        controller_proxy._pyroBind()
    except Exception as e:
        print(f"Error connecting to the server: {e}", file=sys.stderr)
        print("Please ensure the server is running.", file=sys.stderr)
        sys.exit(1)

    app = WorkflowUi(controller_proxy, workflow_path, inventory_path)
    app.run()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python -m ansible_workflow.client <workflow_file> <inventory_file>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
