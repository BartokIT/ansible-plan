from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Tree
from textual.reactive import reactive
from textual import work


class WorkflowApp(App):
    __workflow = None
    BINDINGS = [("d", "toggle_dark", "Toggle dark mode")]
    __workflow = None

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        yield Tree("Workflow")
        yield Footer()

    def on_mount(self) -> None:
        tree = self.query_one(Tree)
        self.build_tree(self.__workflow.get_input_data(), tree.root)
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

    @work(exclusive=True, thread=True)
    def update_tree(self):
        while self.__workflow.is_running():
            self.update_nodes(self.query_one(Tree).root)
            self.query_one(Tree).refresh()
            # self.query_one(Tree).update()
            # self.app.refresh()
            import time
            time.sleep(1)
        self.update_nodes(self.query_one(Tree).root)
        self.query_one(Tree).refresh()

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

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.dark = not self.dark
