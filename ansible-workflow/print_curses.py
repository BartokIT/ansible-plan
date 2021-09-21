import curses
import time
import copy
import threading, time
import itertools

class CursesGui(threading.Thread):
    def __init__(self, workflow, *args, **kwargs):
        super(CursesGui,self).__init__(*args, **kwargs)
        self.__main_window = None
        self.__progress_window = None
        self.__progress_window_width = 35
        self.__window_margin = 3
        self.__workflow = workflow
        self.__row = 0
        self.__terminal = curses.initscr()
        self.__spinner_template = ['-', '/', '|', '\\']
        self.__spinners = {}
        self.__define_color()

        self.__max_y, self.__max_x = self.__terminal.getmaxyx()
        self.__main_window = curses.newwin(self.__max_y - self.__window_margin , self.__max_x - self.__window_margin - self.__progress_window_width, self.__window_margin, self.__window_margin)
        self.__progress_window = curses.newwin(self.__max_y - self.__window_margin, self.__progress_window_width,  self.__window_margin, self.__max_x - self.__window_margin - self.__progress_window_width)
        self.__status_map = {'not_started': {'label': 'not started', 'color': 1},
                             'running': {'label': 'running', 'color': 2},
                             'failed': {'label': 'failed', 'color': 3},
                             'ended': {'label': 'ended', 'color': 4} }
    def run(self):
        self.print()


    def __define_color(self):
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_WHITE, -1) # not started
        curses.init_pair(2, curses.COLOR_YELLOW, -1) # running
        curses.init_pair(3, curses.COLOR_RED, -1) # failed
        curses.init_pair(4, curses.COLOR_GREEN, -1) # ended

    def print_status(self, input_tree, graph):
        import_tree = copy.deepcopy(input_tree)
        del import_tree[0]
        del import_tree[len(import_tree) - 1]

        #self.__main_window.addstr(0, 0, '┬' )
        self.__row = 0
        #self.__main_window.clear()
        self.__progress_window.clear()
        self.__print_tree(import_tree, graph)
        # refresh
        #if self.__first_draw:
        self.__main_window.refresh()
        self.__progress_window.refresh()
        curses.napms(100)

    def print(self):
        # loop over running nodes
        while self.__workflow.is_running():
            self.print_status(self.__workflow.get_input_data(), self.__workflow.get_graph())
            time.sleep(2)
        # leave until
        #self.__progress_window.clear()
        self.print_status(self.__workflow.get_input_data(), self.__workflow.get_graph())
        self.__main_window.refresh()
        self.__progress_window.refresh()
        self.__terminal.getch()
        curses.endwin()


    def __print_tree(self,  nodes, graph, prev_latest=False, level=0, prefix=''):
        for inode in nodes:
            # selection of char for current node
            current_char = '├'
            previous_char = '│'
            next_prefix = ''

            if nodes[-1] == inode: # latest element
                current_char = '└'

                if nodes[0] == inode: # is composed by one element
                    current_char = '─'
                    if not prev_latest:
                        previous_char = '├'
                else:
                    if prev_latest:
                        previous_char = ' '
            elif nodes[0] == inode: # is the first
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


            #print("node: %s | level: %s | prev_latest: %s | prev_char: %s | current_char: %s | next_prefix: #%s# | prefix: #%s#" % ( inode['id'], level, prev_latest, previous_char, current_char, next_prefix, prefix))
            if 'block' in inode:
                self.__print_tree(inode['block'], graph, prev_latest=(nodes[-1] == inode), level=(level + 1), prefix=prefix+next_prefix)
            else:
                if level == 0:
                    previous_char = ''

                # draw tree node
                tree_string =  "%s%s%s %s - %s" % (prefix, previous_char, current_char, inode['id'], inode['import_playbook'])
                self.__main_window.addstr(self.__row, 0, tree_string)

                # draw filling dots
                self.__main_window.addstr(self.__row, len(tree_string.strip()) + 1, "."*(self.__max_x - len(tree_string.strip()) - 5 - self.__window_margin - self.__progress_window_width))

                # draw status
                node_data =  self.__workflow.get_node_datas()[inode['id']]
                node_status = node_data['object'].get_status()
                status_label = self.__status_map[node_status]['label']
                status_color  = self.__status_map[node_status]['color']
                self.__progress_window.addstr(self.__row, 5, "%s" % status_label, curses.color_pair(status_color))

                # draw spinner
                if inode['id'] not in self.__spinners:
                    self.__spinners[inode['id']] = itertools.cycle(self.__spinner_template)

                if node_status == 'running':
                    self.__progress_window.addstr(self.__row, 1, "[%s]" % next(self.__spinners[inode['id']]), curses.color_pair(status_color))

                # draw spinner
                if 'started' in node_data:
                    self.__progress_window.addstr(self.__row, 13, "[%s]" % node_data['started'].strftime("%H:%M:%S"))
                if 'ended' in node_data:
                    self.__progress_window.addstr(self.__row, 22, "-%s]" % node_data['ended'].strftime("%H:%M:%S"))
                self.__row = self.__row + 1
