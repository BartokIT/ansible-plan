from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Tree, Button, RichLog
from textual.reactive import reactive
from textual import work
from textual.containers import Horizontal, Vertical


class WorkflowApp(App):
    __workflow = None
    BINDINGS = [("d", "toggle_dark", "Toggle dark mode"),
                ("s", "start_workflow", "Start Workflow")]
    __workflow = None

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        with Horizontal():
            yield Tree("Workflow")
            with Vertical():
                yield Button("Start", id='start_button')
                yield RichLog(id='details')
        yield Footer()

    def on_mount(self) -> None:
        tree = self.query_one(Tree)
        self.build_tree(self.__workflow.get_input_data(), tree.root)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "start_button":
            self.action_start_workflow()

    def action_start_workflow(self) -> None:
        self.run_worker(self.__workflow.run)
        self.update_tree()

    @classmethod
    def set_workflow(cls, workflow):
        cls.__workflow = workflow

    def build_tree(self, nodes, parent_node):
        for inode in nodes:
            if inode['id'] in ['s', 'e']:
                continue

            if 'block' in inode:
                label = f"Block: {inode.get('id')} ({inode.get('strategy', 'parallel')})"
                new_node = parent_node.add(label, data=inode)
                self.build_tree(inode['block'], new_node)
            else:
                label = f"Playbook: {inode.get('id')} - {inode.get('import_playbook')}"
                parent_node.add_leaf(label, data=inode)

    def _update_and_refresh_tree(self):
        self.update_nodes(self.query_one(Tree).root)
        self.query_one(Tree).refresh()

    @work(exclusive=True, thread=True)
    def update_tree(self):
        import time
        while self.__workflow.is_running():
            self.call_from_thread(self._update_and_refresh_tree)
            time.sleep(1)
        self.call_from_thread(self._update_and_refresh_tree)

    def update_nodes(self, node):
        if not node.data:
            for child in node.children:
                self.update_nodes(child)
            return

        node_id = node.data['id']
        node_data = self.__workflow.get_node_datas().get(node_id)
        if node_data:
            status = node_data['object'].get_status()
            node.set_label(f"{node.label.plain.split(' [')[0]} [{status}]")
            if status == 'running':
                node.label.stylize("yellow")
            elif status == 'ended':
                node.label.stylize("green")
            elif status == 'failed':
                node.label.stylize("red")

        for child in node.children:
            self.update_nodes(child)

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        details = self.query_one('#details', RichLog)
        details.clear()
        if not event.node.data:
            return

        node_id = event.node.data['id']
        node_data = self.__workflow.get_node_datas().get(node_id)
        if node_data:
            details.write(f"ID: {node_id}")
            details.write(f"Status: {node_data['object'].get_status()}")
            details.write(f"Started: {node_data.get('started')}")
            details.write(f"Ended: {node_data.get('ended')}")
            details.write(f"Vars: {node_data.get('vars')}")

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.dark = not self.dark
