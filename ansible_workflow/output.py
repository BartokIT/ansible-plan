import curses
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

class CursesGuiChar(Enum):
    """ Define the character for the application"""
    SERIAL_FIRST_ITEM = '┬'
    SERIAL_MIDDLE_ITEM = '├'
    SERIAL_END_ITEM = '└'


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


class CursesWorkflowOutput(WorkflowOutput, WorkflowListener):
    _log_name = 'curses.log'

    def __init__(self, workflow, event, logging_dir, log_level, cmd_args):
        super().__init__(workflow, event, logging_dir, log_level, cmd_args)
        self.get_workflow().add_event_listener(self)
        self.__main_window = None
        self.__progress_window = None
        self.__progress_window_width = 35
        self.__messages_window_height = 5
        self.__window_margin = 3
        self.__row = 0
        self.__spinner_template = ['-', '/', '|', '\\']
        self.__spinners = {}
        self.__status_map = {NodeStatus.NOT_STARTED: {'label': 'not started', 'color': 1},
                             NodeStatus.RUNNING: {'label': 'running', 'color': 2},
                             NodeStatus.FAILED: {'label': 'failed', 'color': 3},
                             NodeStatus.ENDED: {'label': 'ended', 'color': 4},
                             NodeStatus.SKIPPED: {'label': 'ended', 'color': 5}}
        self.__messages = []
        self.set_run_wrapper(curses.wrapper)

    def __define_color(self):
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_WHITE, -1)  # not started
        curses.init_pair(2, curses.COLOR_YELLOW, -1)  # running
        curses.init_pair(3, curses.COLOR_RED, -1)  # failed
        curses.init_pair(4, curses.COLOR_GREEN, -1)  # ended
        curses.init_pair(5, curses.COLOR_CYAN, -1)  # ended

    def notify_event(self, event: WorkflowEvent):
        self._logger.debug("Event received: %s" % event)

        if event.get_type() == WorkflowEventType.WORKFLOW_EVENT:
            type, text = event.get_event()
            self.__messages.append(text)

    def draw_init(self, stdscr):
        ''' Draw initialization'''
        # define curses color
        stdscr.clear()
        self.__define_color()
        self.__max_y, self.__max_x = stdscr.getmaxyx()
        self._logger.info("Maximum width: %s height: %s" % (self.__max_x, self.__max_y))
        self.__main_window = curses.newwin(self.__max_y - self.__window_margin - self.__messages_window_height,
                                           self.__max_x - self.__window_margin - self.__progress_window_width,
                                           self.__window_margin, self.__window_margin)
        self.__progress_window = curses.newwin(self.__max_y - self.__window_margin - self.__messages_window_height,
                                               self.__progress_window_width,
                                               self.__window_margin,
                                               self.__max_x - self.__window_margin - self.__progress_window_width)
        self.__messages_window = curses.newwin(self.__messages_window_height,
                                               self.__max_x - self.__window_margin,
                                               self.__max_y - self.__window_margin - self.__messages_window_height, self.__window_margin)
        self.__main_window.clear()
        self.__progress_window.clear()
        self.__messages_window.clear()
        self.__print_tree(['_root'])
        self.__main_window.refresh()
        self.__messages_window.refresh()

    def draw_end(self, *args, **kwargs):
        ''' Draw ending'''
        self._logger.info("Wait for exit")
        curses.halfdelay(5)
        curses.napms(self._refresh_interval)
        wait = True

        i = 0
        self.__messages_window.clear()
        self.__main_window.clear()
        for message in self.__messages:
            self._logger.info("Message %s" % message)
            self.__messages_window.addstr(i, 0, "%s" % message)
            i = i + 1
        self.__row = 0
        self.__print_tree(['_root'])
        self.__messages_window.addstr(i + 1, 0, "Press Q to exit")
        self.__main_window.refresh()
        self.__messages_window.refresh()

        while wait:
            char = self.__messages_window.getch()
            if char != curses.ERR:
                if chr(char) == 'q':
                    wait = False
        self._logger.info("End of visualization")

    def draw_step(self):
        ''' Draw the workflow'''
        self.__progress_window.clear()
        self.__row = 0
        self.__print_tree(['_root'])
        self.__progress_window.refresh()

    def draw_pause(self):
        ''' Need to be implemented to pause after a draw step'''
        curses.napms(self._refresh_interval)

    def __print_tree(self, nodes, prev_latest=False, level=0, prefix=''):
        ''' Print the tree representing the workflow'''
        for inode in nodes:
            # selection of char for current node
            current_char = '├'
            previous_char = '│'
            next_prefix = ''

            if nodes[-1] == inode:  # latest element
                current_char = '└'

                if nodes[0] == inode:  # is composed by one element
                    current_char = '─'
                    if not prev_latest:
                        previous_char = '├'
                else:
                    if prev_latest:
                        previous_char = ' '
            elif nodes[0] == inode:  # is the first
                current_char = '┬'
                if prev_latest:
                    previous_char = '└'
                else:
                    previous_char = '├'
            elif prev_latest:
                previous_char = ' '

            # prefix
            if level > 0:
                next_prefix = '│'
                if prev_latest:
                    next_prefix = ' '

            if isinstance(self.get_workflow().get_node_object(inode), BNode):
                self.__print_tree([n for p, n in self.get_workflow().get_original_graph().out_edges(inode)],
                                  prev_latest=(nodes[-1] == inode), level=(level + 1),
                                  prefix=prefix + next_prefix)
            else:
                if level == 0:
                    previous_char = ''

                # draw tree node
                tree_string = "%s%s%s %s - %s" % (prefix, previous_char, current_char,
                                                  self.get_workflow().get_node_object(inode).get_id(),
                                                  self.get_workflow().get_node_object(inode).get_playbook())
                self.__main_window.addstr(self.__row, 0, tree_string)

                # draw filling dots
                self.__main_window.addstr(self.__row,
                                          len(tree_string.strip()) + 1,
                                          "." * (self.__max_x - len(tree_string.strip()) - 5 - self.__window_margin - self.__progress_window_width))

                # draw status
                node_object = self.get_workflow().get_node_object(inode)
                node_status = node_object.get_status()
                status_label = self.__status_map[node_status]['label']
                status_color = self.__status_map[node_status]['color']
                self.__progress_window.addstr(self.__row, 5, "%s" % status_label, curses.color_pair(status_color))

                # draw spinner
                if inode not in self.__spinners:
                    self.__spinners[node_object.get_id()] = itertools.cycle(self.__spinner_template)

                if node_status == NodeStatus.RUNNING:
                    self.__progress_window.addstr(self.__row, 1, "[%s]" % next(self.__spinners[inode]), curses.color_pair(status_color))

                # draw spinner
                if node_object.get_telemetry()['started']:
                    self.__progress_window.addstr(self.__row, 13, "[%s]" % node_object.get_telemetry()['started'])
                if node_object.get_telemetry()['ended']:
                    self.__progress_window.addstr(self.__row, 22, "-%s]" % node_object.get_telemetry()['ended'])
                self.__row = self.__row + 1



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
