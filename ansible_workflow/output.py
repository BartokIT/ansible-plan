from enum import Enum
import threading
import itertools
import os
import logging
import abc
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", message="networkx backend defined more than once: nx-loopback")
    import networkx as nx

from typing import Callable
from .workflow import (AnsibleWorkflow, BNode, NodeStatus, PNode, WorkflowEvent, WorkflowListener,
                       WorkflowStatus, WorkflowEventType)
from rich.console import Console
from rich.table import Table
import sys
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static, Tree, RichLog, Rule, DataTable
from textual.containers import Container, Horizontal, Vertical
from textual import work
from textual.reactive import reactive
import time

class WorkflowOutput(threading.Thread):
    '''
    A general workflow output class to be implemented by subclasses
    '''
    def __init__(self, workflow, event, logging_dir, log_level, cmd_args):
        threading.Thread.__init__(self)
        self._define_logger(logging_dir, log_level)
        self.__workflow: AnsibleWorkflow = workflow
        self._refresh_interval = 200
        self.__verify_only = cmd_args.verify_only
        self.event: threading.Event = event

    def is_verify_only(self):
        return self.__verify_only

    def get_remaining_command_line(self):
        next_command_line = ""
        someone_failed = False
        next_run_node = []
        for node_id in self.get_workflow().get_nodes():
            data = self.get_workflow().get_node(node_id)[1]
            node = data['object']
            if isinstance(node, PNode) and (node.get_status() != NodeStatus.FAILED and node.get_status() != NodeStatus.NOT_STARTED):
                next_run_node.append(node.get_id())
            elif isinstance(node, PNode) and node.get_status() == NodeStatus.FAILED:
                someone_failed =  True

        skip_argument=False
        for arg in sys.argv:
            if arg == '--skip-nodes':
                skip_argument=True
                continue
            if skip_argument:
                skip_argument=False
                continue
            next_command_line += ' %s' % arg

        if next_run_node and someone_failed:
            next_command_line += ' --skip-nodes %s' % ','.join(next_run_node)
            return next_command_line
        else:
            return ""



    def get_workflow(self):
        return self.__workflow

    def _define_logger(self, logging_dir, level):
        logger_name = self.__class__.__name__
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
        self.__run_wrapper = self.run_wrapper

    def set_run_wrapper(self, run_wrapper: Callable):
        self.__run_wrapper = run_wrapper

    def run_wrapper(self, run_function: Callable):
        run_function()

    def run(self):
        self._logger.info("WorkflowOutput run")
        self.__run_wrapper(self._draw_init)

    def _draw_init(self, *args, **kwargs):
        ''' Loop over the workflows status until is running and call draw step and pause method'''
        self.draw_init(*args, **kwargs)
        while self.get_workflow().get_running_status() != WorkflowStatus.ENDED and self.get_workflow().get_running_status() != WorkflowStatus.FAILED:
            self.draw_step()
            self.draw_pause()
        self.draw_end(*args, **kwargs)

    def set_refresh_interval(self, ms):
        self._refresh_interval = ms

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


class StdoutWorkflowOutput(WorkflowOutput, WorkflowListener):
    _log_name = 'console.log'

    def __init__(self, workflow, event, logging_dir, log_level, cmd_args):
        super().__init__(workflow, event, logging_dir, log_level, cmd_args)
        self._refresh_interval = 2
        self.get_workflow().add_event_listener(self)
        self.__console = Console()
        self.__events_stream = []
        self.__waiting_input = False
        self.__interactive_retry = cmd_args.interactive_retry
        self.__doubtful_mode = cmd_args.doubtful_mode
        # import structout as so
        # so.gprint(self.get_workflow().get_graph())
        # sys.exit()
    def draw_init(self):
        self._logger.debug("Initializing stdout output")

        table = Table(title="Workflow nodes")

        table.add_column("Node", justify="left", style="cyan", no_wrap=True)
        table.add_column("Playbook", style="bright_magenta")
        # table.add_column("Level", style="bright_cyan")
        # table.add_column("Block", style="green")
        # table.add_column("Strategy", style="yellow")
        table.add_column("Ref.", style="cyan")
        table.add_column("Status")

        for node_id in self.get_workflow().get_nodes():
            data = self.get_workflow().get_node(node_id)[1]
            node = data['object']
            if isinstance(node, PNode):
                table.add_row("%s" % node.get_id(),
                              "%s" % node.get_playbook(),
                              # "%s%s" % (' ' * data['level'], data['level']),
                              # "%s" % data['block']['block_id'],
                              # "%s" % data['block']['strategy'],
                              "%s" % node.get_reference(),
                              "%s" % self._render_status(node.get_status()))

                table.add_row("",
                            "[white]%s[/]" % node.get_description(),
                            "",
                            "")

        self.__console.print(table)
        self.__console.print("")
        self.__console.print("[italic]Running[/] ...", justify="center")

    def draw_step(self):
        pass

    def draw_pause(self):
        ''' Non blocking thread wait'''
        self.event.wait(self._refresh_interval)

    def draw_end(self):
        table = Table(title="Running recap")

        table.add_column("Node", justify="left", style="cyan", no_wrap=True)
        table.add_column("Playbook", style="bright_magenta")
        # table.add_column("Level", style="bright_cyan")
        table.add_column("Block", style="green")
        # table.add_column("Strategy", style="yellow")
        table.add_column("Ref.", style="cyan")
        table.add_column("Status")

        for node_id in self.get_workflow().get_nodes():
            data = self.get_workflow().get_node(node_id)[1]
            node = data['object']
            if isinstance(node, PNode):
                table.add_row("%s" % node.get_id(),
                              "%s" % node.get_playbook(),
                              # "%s%s" % (' ' * data['level'], data['level']),
                              "%s" % data['block']['block_id'],
                              # "%s" % data['block']['strategy'],
                              "%s" % node.get_reference(),
                              "%s" % self._render_status(node.get_status()))

        self.__console.print(table)
        self.__console.print("")
        next_run = self.get_remaining_command_line()
        if next_run and not self.is_verify_only():
            self.__console.print("To execute only the failed jobs, launch the following command:")
            self.__console.print("[yellow]%s[/]" % next_run)

        self._logger.debug("stdout output ends")

    def _render_status(self, status):
        if status == NodeStatus.RUNNING:
            return '[yellow]started[/]'
        elif status == NodeStatus.ENDED:
            return '[green]completed[/]'
        elif status == NodeStatus.FAILED:
            return '[bright_red]failed[/]'
        elif status == NodeStatus.NOT_STARTED:
            return '[white]not started[/]'
        elif status == NodeStatus.SKIPPED:
            return '[cyan]skipped[/]'
        else:
            return 'unknown'

    def _print_event(self, event: WorkflowEvent):
        table = Table(show_header=False, show_footer=False, show_lines=False, show_edge=False)
        table.add_column()
        table.add_column()
        if event.get_type() == WorkflowEventType.NODE_EVENT:
            type, node = event.get_event()
            if isinstance(node, PNode) and type != NodeStatus.PRE_RUNNING:
                status = self._render_status(type)
                table.add_row("%s - %s" % (node.get_telemetry()['started'] if node.get_telemetry()['started'] else '   ...  ',
                                           node.get_telemetry()['ended'] if node.get_telemetry()['ended'] else '   ...  '),
                              "Node [cyan]%s[/] is %s" % (node.get_id(), status))
                self.__console.print(table)
        elif event.get_type() == WorkflowEventType.WORKFLOW_EVENT:
            type, text = event.get_event()
            if type == WorkflowStatus.FAILED:
                table.add_row("%s - %s" % ('   ...  ', '   ...  '),
                              "[bright_red]%s[/]" % text)

            self.__console.print(table)

    def notify_event(self, event: WorkflowEvent):
        self._logger.debug("Event received: %s" % event)
        if event.get_type() == WorkflowEventType.NODE_EVENT:
            self._print_event(event)
            type, node = event.get_event()
            if node.get_status() == NodeStatus.FAILED:
                if self.__interactive_retry:
                    y_or_n = ''

                    table = Table(show_header=False, show_footer=False, show_lines=False, show_edge=False)
                    table.add_column()
                    table.add_column(justify="right")
                    table.add_column()

                    table.add_row('','','')
                    table.add_section()
                    table.add_row('','[bright_magenta]Node[/]',f'[cyan]{node.ident}[/]')
                    table.add_row('','[bright_magenta]Reference[/]',node.get_reference())
                    table.add_row('','     [bright_magenta]Description[/]',node.get_description())
                    table.add_section()
                    table.add_row('','','')
                    self.__console.print(table)

                    while y_or_n.lower() not in ['y', 'n', 's']:
                        if y_or_n == 'l':
                            fname = os.path.join(self._logging_dir, node.ident, 'stdout')
                            table = Table(show_header=False, show_footer=False, show_lines=False, show_edge=False)
                            table.add_column()
                            table.add_column(justify="right")
                            table.add_column()
                            table.add_section()
                            with open(fname) as file:
                                linucountmax = 30
                                lines = file.readlines()
                                i = 0 if len(lines)<linucountmax else len(lines)-linucountmax
                                if len(lines)>=linucountmax:
                                    lines = lines[-linucountmax:]
                                for line in lines:
                                    table.add_row('', f'     [bright_magenta]Log [/]({i: >5})',line[:-1])
                                    i+=1
                                table.add_section()
                                table.add_row('', f'[bright_magenta]Full log[/]',fname)
                                table.add_section()
                                table.add_row('','','')
                            self.__console.print(table)
                        table = Table(show_header=False, show_footer=False, show_lines=False, show_edge=False)
                        table.add_column()
                        table.add_column(justify="right")
                        table.add_column()
                        table.add_row('', '?               ', '[white]-> Do you want to restart? [green]y[/](yes) / [bright_red]n[/](no) / [cyan]s[/](skip) / [bright_magenta]l[/](logs): ')
                        self.__console.print(table)
                        y_or_n = self.__console.input('  │ >>>>>>>>>>>>>>>> │ ')
                    if y_or_n == 'y':
                        self.get_workflow().add_running_node(node.get_id())
                        self.get_workflow().run_node(node.get_id())
                    elif y_or_n == 's':
                        node.set_skipped()
                        self.get_workflow().add_running_node(node.get_id())

            elif type == NodeStatus.PRE_RUNNING and not node.is_skipped():
                if self.__doubtful_mode:
                    y_or_n = ''
                    table = Table(show_header=False, show_footer=False, show_lines=False, show_edge=False)
                    table.add_column()
                    table.add_column(justify="right")
                    table.add_column()
                    table.add_row('', '?               ', f'[white]-> Do you want to run [cyan]{node.get_id()}[/]? [green]y[/](yes) / [cyan]s[/](skip): ')
                    self.__console.print(table)
                    while y_or_n.lower() != 'y' and y_or_n.lower() != 's':
                        y_or_n = self.__console.input('  │ >>>>>>>>>>>>>>>> │ ')
                    if y_or_n == 's':
                        node.set_skipped()
        else:
            self._print_event(event)


def nudge(pos, x_shift, y_shift):
    return {n: (x + x_shift, y + y_shift) for n, (x, y) in pos.items()}



class PngDrawflowOutput(WorkflowOutput):
    _log_name = 'pngdraw.log'

    def __init__(self, workflow, event, logging_dir, log_level, cmd_args):
        super().__init__(workflow, event, logging_dir, log_level, cmd_args)
        self._refresh_interval = 2
        self.__dpi = cmd_args.draw_dpi
        self.__size = cmd_args.draw_size

    def draw_init(self):
        self._logger.debug("Initializing stdout output")
        self.draw_graph(False, self.__size, self.__dpi)

    def draw_graph(self, colorize_status=False, size=15, dpi=72):
        import matplotlib.pyplot as plt
        plt.figure(1, figsize=(size, size))
        # calculate node position using graphviz
        pos = nx.nx_agraph.graphviz_layout(self.get_workflow().get_graph(), prog='neato')

        # draw all nodes

        if colorize_status:
            ended_nodes = [n for n in list(self.get_workflow().get_graph().nodes()) if isinstance(self.get_workflow().get_node_object(n), PNode)
                           and self.get_workflow().get_node_object(n).get_status() == NodeStatus.ENDED]
            failed_nodes = [n for n in list(self.get_workflow().get_graph().nodes()) if isinstance(self.get_workflow().get_node_object(n), PNode)
                            and self.get_workflow().get_node_object(n).get_status() == NodeStatus.FAILED]
            nx.draw_networkx_nodes(self.get_workflow().get_graph(), pos, nodelist=ended_nodes, node_size=250, node_color="#580")
            nx.draw_networkx_nodes(self.get_workflow().get_graph(), pos, nodelist=failed_nodes, node_size=250, node_color="#a10")
        else:
            play_nodes = [n for n in list(self.get_workflow().get_graph().nodes()) if isinstance(self.get_workflow().get_node_object(n), PNode)]
            nx.draw_networkx_nodes(self.get_workflow().get_graph(), pos, nodelist=play_nodes, node_size=250)

        # draw block nodes differently
        block_nodes = [n for n in list(self.get_workflow().get_graph().nodes()) if isinstance(self.get_workflow().get_node_object(n), BNode)
                       and self.get_workflow().get_node_object(n).get_id() not in ['_root', '_s', '_e']]
        nx.draw_networkx_nodes(self.get_workflow().get_graph(), pos, nodelist=block_nodes, node_size=150, node_color="#777")

        # draw start and end node differently
        nx.draw_networkx_nodes(self.get_workflow().get_graph(), pos, nodelist=['_s'], node_size=500, node_color="#580")
        nx.draw_networkx_nodes(self.get_workflow().get_graph(), pos, nodelist=['_e'], node_size=500, node_color="#a10")
        # draw edges
        nx.draw_networkx_edges(self.get_workflow().get_graph(), pos, width=1, alpha=0.9, edge_color="#777")

        # draw labels
        label_position = nudge(pos, 0, 0)
        # labels = {n: '' if isinstance(self.get_workflow().get_node_object(n), BNode) else os.path.basename(
        #           self.get_workflow().get_node_object(n).get_playbook()) for n in list(self.get_workflow().get_graph().nodes())}
        labels = {n: '' if isinstance(self.get_workflow().get_node_object(n), BNode) else
                  self.get_workflow().get_node_object(n).get_id() for n in list(self.get_workflow().get_graph().nodes())}
        nx.draw_networkx_labels(self.get_workflow().get_graph(), labels=labels, pos=label_position, font_size=10)

        plt.savefig(os.path.join(self._logging_dir, 'workflow.png'), dpi=dpi)
        # plt.savefig('/var/www/html/installers/workflow.png')

    def draw_step(self):
        pass

    def draw_pause(self):
        ''' Non blocking thread wait'''
        self.event.wait(self._refresh_interval)

    def draw_end(self):
        self.draw_graph(True)
        self._logger.debug("png output ends")


class TextualWorkflowOutput(WorkflowOutput, WorkflowListener):
    _log_name = 'textual.log'

    def __init__(self, workflow, event, logging_dir, log_level, cmd_args):
        super().__init__(workflow, event, logging_dir, log_level, cmd_args)
        # We are not using the base class run loop, so we don't need to set the run_wrapper
        self.get_workflow().add_event_listener(self)
        self.app = self.WorkflowApp(self)

    def run(self, start_node, end_node, verify_only):
        """
        This method is called directly from __main__.py for textual mode.
        It launches the Textual app. The app itself will then start the
        workflow in a background thread.
        """
        self.app.start_node = start_node
        self.app.end_node = end_node
        self.app.verify_only = verify_only
        self.app.run()

    def notify_event(self, event: WorkflowEvent):
        # This is called from the workflow thread
        self._logger.debug("Event received: %s" % event)
        self.app.handle_workflow_event(event)

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
            self.tree_nodes = {}
            self.spinner_icons = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
            self.status_icons = {
                NodeStatus.NOT_STARTED: "○",
                NodeStatus.PRE_RUNNING: "[yellow]…[/yellow]",
                NodeStatus.RUNNING: "[yellow]○[/yellow]",
                NodeStatus.ENDED: "[green]✔[/green]",
                NodeStatus.FAILED: "[red]✖[/red]",
                NodeStatus.SKIPPED: "[cyan]»[/cyan]",
            }
            self.node_spinners = {}
            self.stdout_watcher = None

        def compose(self) -> ComposeResult:
            yield Header()
            with Horizontal():
                yield Tree("Workflow", id="workflow_tree", classes="sidebar")
                with Vertical():
                    yield DataTable(id="node_details")
                    yield Rule()
                    yield RichLog(id="playbook_stdout", markup=True)
            yield Footer()

        def on_mount(self) -> None:
            workflow = self.outer_instance.get_workflow()
            tree = self.query_one(Tree)
            root_node_id = "_root"
            root_node = tree.root
            root_node.data = root_node_id
            root_node.set_label(f"{self.status_icons.get(NodeStatus.NOT_STARTED, ' ')} Workflow")
            self.tree_nodes[root_node_id] = root_node
            details_table = self.query_one("#node_details", DataTable)
            details_table.add_columns("Property", "Value")
            self._build_tree(workflow, root_node_id, root_node)
            tree.root.expand_all()
            self.run_workflow()

        @work(thread=True)
        def run_workflow(self):
            self.outer_instance.get_workflow().run(
                start_node=self.start_node,
                end_node=self.end_node,
                verify_only=self.verify_only
            )

        def _build_tree(self, workflow, node_id, tree_node):
            for child_id in workflow.get_original_graph().successors(node_id):
                if child_id in ['_s', '_e']:
                    continue
                child_node_obj = workflow.get_node_object(child_id)
                allow_expand = isinstance(child_node_obj, BNode)
                if isinstance(child_node_obj, BNode):
                    label = f"[b]{child_node_obj.get_id()}[/b]"
                else:
                    icon = self.status_icons.get(child_node_obj.get_status(), " ")
                    label = f"{icon} {child_node_obj.get_id()}"

                child_tree_node = tree_node.add(label, data=child_id, allow_expand=allow_expand)
                self.tree_nodes[child_id] = child_tree_node

                if workflow.get_original_graph().out_degree(child_id) > 0:
                    self._build_tree(workflow, child_id, child_tree_node)

        def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
            if self.stdout_watcher:
                self.stdout_watcher.cancel()
                self.stdout_watcher = None

            node_id = event.node.data
            workflow = self.outer_instance.get_workflow()
            node_obj = workflow.get_node_object(node_id)
            details_table = self.query_one("#node_details", DataTable)
            details_table.clear()

            if isinstance(node_obj, PNode):
                details_table.add_row("ID", node_obj.get_id())
                details_table.add_row("Playbook", node_obj.get_playbook())
                details_table.add_row("Inventory", getattr(node_obj, '_PNode__inventory', 'N/A'))
                details_table.add_row("Description", node_obj.get_description())
                details_table.add_row("Reference", node_obj.get_reference())
                self.show_stdout(node_obj)
                if node_obj.get_status() == NodeStatus.RUNNING:
                    self.stdout_watcher = self.watch_stdout(node_obj)
            elif isinstance(node_obj, BNode):
                stdout_log = self.query_one("#playbook_stdout", RichLog)
                stdout_log.clear()
                node_data = workflow.get_node(node_id)[1]
                details_table.add_row("ID", node_obj.get_id())
                details_table.add_row("Type", "Block")
                details_table.add_row("Strategy", node_data.get('block', {}).get('strategy', 'N/A'))
            else:
                # Details for root node or other types
                details_table.add_row("ID", node_obj.get_id())

        def handle_workflow_event(self, event: WorkflowEvent):
            if event.get_type() == WorkflowEventType.NODE_EVENT:
                status, node = event.get_event()
                node_id = node.get_id()

                if node_id in self.tree_nodes:
                    tree_node = self.tree_nodes[node_id]

                    if status == NodeStatus.RUNNING:
                        if node_id not in self.node_spinners:
                            self.node_spinners[node_id] = self.update_spinner(node)
                    else:
                        if node_id in self.node_spinners:
                            self.node_spinners[node_id].cancel()
                            del self.node_spinners[node_id]

                        if isinstance(node, BNode):
                            label = f"[b]{node_id}[/b]"
                        else:
                            icon = self.status_icons.get(status, " ")
                            label = f"{icon} {node_id}"
                        tree_node.set_label(label)

            elif event.get_type() == WorkflowEventType.WORKFLOW_EVENT:
                status, content = event.get_event()
                # You can add logic here to handle workflow-level events, e.g., display a notification
                pass

        @work(thread=True)
        def update_spinner(self, node: PNode):
            node_id = node.get_id()
            tree_node = self.tree_nodes[node_id]
            spinner_cycle = itertools.cycle(self.spinner_icons)
            is_bnode = isinstance(node, BNode)

            while node.get_status() == NodeStatus.RUNNING:
                icon_char = next(spinner_cycle)
                icon = f"[yellow]{icon_char}[/yellow]"
                if is_bnode:
                    label = f"{icon} [b]{node_id}[/b]"
                else:
                    label = f"{icon} {node_id}"
                tree_node.set_label(label)
                time.sleep(0.1)

        @work(exclusive=True, thread=True)
        def show_stdout(self, node: PNode):
            """Reads and displays the entire stdout for a given node."""
            stdout_log = self.query_one("#playbook_stdout", RichLog)
            stdout_log.display = False
            stdout_log.display = True
            stdout_log.clear()

            artifact_dir = self.outer_instance.get_workflow().get_logging_dir()
            # The 'ident' is the actual directory name used by ansible-runner, which
            # can be different from the node_id if the node is re-run.
            ident = getattr(node, 'ident', node.get_id())
            stdout_path = os.path.join(artifact_dir, ident, "stdout")

            self.outer_instance._logger.info(f"Showing stdout for node {node.get_id()} from {stdout_path}")

            if os.path.exists(stdout_path):
                with open(stdout_path, "r") as f:
                    content = f.read()
                    width = stdout_log.size.width
                    padded_lines = [line.ljust(width) for line in content.splitlines()]
                    padded_content = "\n".join(padded_lines)
                    stdout_log.write(padded_content)
            else:
                stdout_log.write("No standard output available for this node (it may not have run yet).")

        @work(exclusive=True, thread=True)
        def watch_stdout(self, node: PNode):
            stdout_log = self.query_one("#playbook_stdout", RichLog)
            stdout_log.clear()

            artifact_dir = self.outer_instance.get_workflow().get_logging_dir()
            ident = getattr(node, 'ident', node.get_id())
            stdout_path = os.path.join(artifact_dir, ident, "stdout")

            self.outer_instance._logger.info(f"Watching stdout for node {node.get_id()} at {stdout_path}")

            last_pos = 0
            while node.get_status() == NodeStatus.RUNNING:
                if os.path.exists(stdout_path):
                    with open(stdout_path, "r") as f:
                        f.seek(last_pos)
                        new_content = f.read()
                        if new_content:
                            stdout_log.write(new_content)
                            last_pos = f.tell()
                time.sleep(0.5)

            # Final read
            if os.path.exists(stdout_path):
                with open(stdout_path, "r") as f:
                    f.seek(last_pos)
                    new_content = f.read()
                    if new_content:
                        stdout_log.write(new_content)
