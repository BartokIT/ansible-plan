import Pyro5.api
from itertools import cycle
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Tree, Log, Button
from textual.containers import Horizontal, Vertical
from textual import work
import sys
import os
import asyncio

# The name of the Pyro object registered on the name server
PYRO_NAME = "ansible.workflow"

class WorkflowUi(App):
    """A Textual app to manage and monitor Ansible workflows."""

    CSS_PATH = "client.css"

    BINDINGS = [
        ("d", "toggle_dark", "Toggle dark mode"),
        ("q", "quit", "Quit"),
    ]

    def __init__(self, workflow_proxy):
        super().__init__()
        self.workflow = workflow_proxy
        self.polling_timer = None
        self.spinner_cycle = cycle(["⏳", "⌛"])
        self.log_stream_worker = None

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        with Horizontal():
            with Vertical(id="tree-container"):
                yield Tree("Workflow", id="workflow-tree", data={'id': 'root', 'label': 'Workflow'})
            with Vertical(id="main-container"):
                with Horizontal(id="buttons-container"):
                    yield Button("Start Workflow", id="start-button", variant="success")
                    yield Button("Stop Workflow", id="stop-button", variant="error")
                yield Log(id="output-log", auto_scroll=True)
        yield Footer()

    def on_mount(self) -> None:
        """Called when app is mounted."""
        self.build_tree()
        self.polling_timer = self.set_interval(1, self.update_statuses)

    def build_tree(self) -> None:
        """Builds the workflow tree from data fetched from the server."""
        tree = self.query_one(Tree)
        tree.clear()
        try:
            workflow_data = self.workflow.get_input_data()
            actual_workflow = workflow_data[1:-1] # Skip 's' and 'e' nodes
            self._recursive_build_tree(tree.root, actual_workflow)
            tree.root.expand()
        except Exception as e:
            self.query_one("#output-log").write(f"Error building tree: {e}")

    def _recursive_build_tree(self, parent_node, data):
        for item in data:
            node_id = item['id']
            if 'block' in item:
                label = item.get('name', f"Block: {node_id}")
                new_node = parent_node.add(label, data={'id': node_id, 'label': label})
                self._recursive_build_tree(new_node, item['block'])
            else:
                playbook_name = os.path.basename(item.get('import_playbook', 'Unknown Playbook'))
                label = item.get('name', playbook_name)
                new_node = parent_node.add_leaf(label, data={'id': node_id, 'label': label})

    @work(exclusive=True)
    async def update_statuses(self) -> None:
        """Polls the server for status updates and refreshes the tree."""
        try:
            statuses = self.workflow.get_nodes_status()
            tree = self.query_one(Tree)

            status_map = {
                'not_started': "⚪",
                'running': f"{next(self.spinner_cycle)}",
                'ended': "✅",
                'failed': "❌",
                'stopped': "🛑",
            }

            def update_node_label(node):
                if not hasattr(node, 'data') or node.data is None or 'id' not in node.data:
                    return

                node_id = node.data.get('id')
                base_label = node.data.get('label')

                if node_id and base_label and node_id in statuses:
                    status = statuses[node_id]['status']
                    icon = status_map.get(status, '❔')
                    new_label = f"{icon} {base_label}"
                    node.set_label(new_label)

                for child_node in node.children:
                    update_node_label(child_node)

            update_node_label(tree.root)

        except Exception as e:
            if self.polling_timer:
                self.polling_timer.stop()
            self.query_one("#output-log").write(f"Error updating statuses: {e}\nPolling stopped.")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button press events."""
        if event.button.id == "start-button":
            try:
                self.workflow.run()
                self.query_one("#output-log").write("Workflow start signal sent.")
            except Exception as e:
                self.query_one("#output-log").write(f"Error starting workflow: {e}")
        elif event.button.id == "stop-button":
            try:
                self.workflow.stop()
                self.query_one("#output-log").write("Workflow stop signal sent.")
            except Exception as e:
                self.query_one("#output-log").write(f"Error stopping workflow: {e}")

    async def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Handle tree node selection and start tailing the log."""
        if self.log_stream_worker:
            self.log_stream_worker.cancel()

        node_id = event.node.data.get('id')
        if not node_id or node_id == 'root':
            return

        log_widget = self.query_one("#output-log")
        log_widget.clear()
        log_widget.write(f"--- Tailing output for {event.node.label} ---")

        self.log_stream_worker = self.stream_log(node_id)

    @work(exclusive=False)
    async def stream_log(self, node_id: str) -> None:
        """Worker to stream log output for a node."""
        log_widget = self.query_one("#output-log")
        offset = 0
        while self.is_running:
            try:
                new_content, new_offset = self.workflow.tail_playbook_output(node_id, offset)
                if new_content:
                    log_widget.write(new_content)
                offset = new_offset
                await asyncio.sleep(1)
            except Exception as e:
                log_widget.write(f"\nError tailing log: {e}")
                break


def main():
    """Main function to run the TUI client."""
    try:
        workflow_proxy = Pyro5.api.Proxy(f"PYRONAME:{PYRO_NAME}")
        workflow_proxy._pyroBind()
    except Exception as e:
        print(f"Error connecting to the server: {e}", file=sys.stderr)
        print("Please ensure the server is running and a Pyro name server is active.", file=sys.stderr)
        sys.exit(1)

    app = WorkflowUi(workflow_proxy)
    app.run()

if __name__ == "__main__":
    main()
