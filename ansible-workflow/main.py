#!/bin/env python3
import ansible_runner
import networkx as nx
from numpy import e
import yaml
import matplotlib.pyplot as plt
import random
import string
import inspect
import abc
import time
import os
import copy
import curses

class Node():
    __metaclass__ = abc.ABCMeta

    def __init__(self, id):
        self.__id = id

    def get_id(self):
        return self.__id

    def __eq__(self, other):
        return self.__id == other.get_id()

    def __hash__(self):
        return hash(self.__id)

    @abc.abstractmethod
    def get_status(self):
        return 'ended'

class BNode(Node):
    def get_status(self):
        return 'ended'

class PNode(Node):
    def __init__(self, id, playbook, inventory, extravars={}):
        super(PNode, self).__init__(id)
        self.__playbook = playbook
        self.__inventory = inventory
        self.__extravars = extravars
        self.__thread = None
        self.__runner = None

    def get_status(self):
        if self.__thread is None:
            return 'not_started'
        else:
            # print("Node %s status is %s - error is %s" % (self.get_id(), self.__runner.errored, self.__runner.status ))
            if self.__thread.is_alive():
                return 'running'
            elif self.is_failed():
                return 'failed'
            else:
                return 'ended'

    def is_failed(self):
        return self.__runner.status == 'failed'

    def get_playbook(self):
        return self.__playbook

    def run(self):
        self.__thread, self.__runner = ansible_runner.run_async(playbook=self.__playbook, inventory=self.__inventory, extravars=self.__extravars,  quiet=True)

def nudge(pos, x_shift, y_shift):
    return {n:(x + x_shift, y + y_shift) for n,(x,y) in pos.items()}

class AnsibleWorkflow():
    def __init__(self, workflow, inventory):
        self.__graph = nx.DiGraph()
        self.__allowed_node_keys = set(['block', 'import_playbook', 'name', 'strategy', 'id', 'vars'])
        self.__workflow_filename = workflow
        self.__inventory_filename = inventory
        self.__running_nodes = ['s']
        self.__running_statues = 'not_started'
        self.__row = 0

        # import data from file
        self.__import_file(self.__workflow_filename)
        self._import_nodes(self.__input_file_data, [], strategy='serial')
        print("%s" % self.__graph.nodes)

    def _get_input(self, filename):
        with open(filename, 'r') as stream:
            data_loaded = yaml.safe_load(stream)
        return data_loaded

    def __import_file(self, filename):
        input_file_data = self._get_input(filename)
        input_file_data.insert(0, dict(id='s', block=[]))
        input_file_data.append(dict(id='e', block=[]))
        self.__input_file_data = input_file_data

    def draw_graph(self):
        # calculate node position using graphviz
        pos=nx.nx_agraph.graphviz_layout(self.__graph)

        # draw all nodes
        nx.draw_networkx_nodes(self.__graph, pos, node_size=250)

        # draw block nodes differently
        block_nodes=[n for n, d in  list(self.__graph.nodes(data=True)) if isinstance(d['data'], BNode) and d['data'].get_id() != 's' and d['data'].get_id() != 'e']
        nx.draw_networkx_nodes(self.__graph, pos, nodelist=block_nodes, node_size=350, node_color="#777")

        # draw start and end node differently
        nx.draw_networkx_nodes(self.__graph, pos, nodelist=['s'], node_size=500, node_color="#580")
        nx.draw_networkx_nodes(self.__graph, pos, nodelist=['e'], node_size=500, node_color="#a10")
        # draw edges
        nx.draw_networkx_edges(self.__graph, pos, width=1, alpha=0.9, edge_color="#777")

        # draw labels
        label_position = nudge(pos, 0, 20)
        labels = {n: '' if isinstance(d['data'], BNode) else os.path.basename(d['data'].get_playbook()) for n, d in  list(self.__graph.nodes(data=True)) }
        nx.draw_networkx_labels(self.__graph,labels=labels, pos=label_position, font_size=10)

        plt.savefig(self.__workflow_filename + '.png')

    def __print_graph(self, window):
        import_tree = copy.deepcopy(self.__input_file_data)
        del import_tree[0]
        del import_tree[len(import_tree) - 1]
        self.__print_tree(window, import_tree)


    def __print_tree(self, window, nodes, prev_latest=False, level=0, prefix=''):
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
                self.__print_tree(window, inode['block'], prev_latest=(nodes[-1] == inode), level=(level + 1), prefix=prefix+next_prefix)
            else:
                if level == 0:
                    previous_char = ''
                window.addstr(self.__row, 0, "%s%s%s %s [%s]" % (prefix, previous_char, current_char, inode['import_playbook'], self.__graph.nodes[inode['id']]['data'].get_status()))
                self.__row = self.__row + 1

    def run_graphically(self):
        terminal = curses.initscr()
        max_y, max_x = terminal.getmaxyx()
        main_win = curses.newwin(max_y, max_x, 0, 0)
        if self.__running_statues != 'not_started':
            raise Exception("Already running")
        self.__running_statues = 'started'
        while len(self.__running_nodes):
            self.__row = 0
            self.__run_step()
            self.__print_graph(main_win)
            main_win.refresh()
            main_win.clear()
            curses.napms(2000)
            terminal.refresh()
            #terminal.clear()
        curses.endwin()
        self.__running_statues = 'ended'


    def _check_node_syntax(self, node):
        remaining_keys = set(node.keys()) - self.__allowed_node_keys
        if remaining_keys:
            raise Exception("The following keys are not allowed %s" % remaining_keys)

    def _import_nodes(self, to_be_imported, parent_nodes, strategy='serial'):
        indentation = '\t' * (len(inspect.stack(0)) - 4)
        # init to loop over the structure
        zero_outdegree_nodes = []
        i=1
        for inode in to_be_imported:
            # basic syntax check of structure's node
            self._check_node_syntax(inode)

            # generate a node identifier and set to the node
            gnode_id= inode.get('id',''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5)))
            gnode_id= str(gnode_id)
            inode['id']= gnode_id

            print("-->> %s node: %s       parents: %s       zero_outdegree: %s" % (indentation, inode['id'], [p.get_id() for p in parent_nodes], [p.get_id() for p in  zero_outdegree_nodes]))

            for parent_node in parent_nodes:
                self.__graph.add_edge(parent_node.get_id(), gnode_id)

            if strategy == 'serial':
                parent_nodes = []
                for zero_outdegree_node in zero_outdegree_nodes:
                    self.__graph.add_edge(zero_outdegree_node.get_id(), gnode_id)
                zero_outdegree_nodes = []

            # generate the object representing the graph
            if 'block' in inode:
                gnode = BNode(gnode_id)
                block_sub_nodes = self._import_nodes(inode['block'], [gnode,], inode.get('strategy','parallel'))
            else:
                playbook=os.path.abspath(inode['import_playbook'])
                inventory=os.path.abspath(inode.get('inventory', self.__inventory_filename))
                extravars=inode.get('vars', {})
                gnode = PNode(gnode_id, playbook=playbook, inventory=inventory, extravars=extravars)
                print("     %s node: %s       playbook: %s     inventory: %s    vars: %s" % (indentation, gnode_id, playbook, inventory, extravars))

            # the node specification is added
            self.__graph.add_node(gnode_id, data=gnode)


            if 'block' not in inode:
                if strategy == 'parallel' or (strategy == 'serial' and inode == to_be_imported[-1]):
                    zero_outdegree_nodes.append(gnode)
            else:
                zero_outdegree_nodes.extend(block_sub_nodes)

            # if the strategy is serial
            if strategy == 'serial':
                if 'block' in inode and len(inode['block']) > 0:
                    # add the node from the subtree as parent
                    parent_nodes = block_sub_nodes
                else:
                    # or add current node as the parent
                    parent_nodes = [gnode,]
            print("<<-- %s node: %s       parents: %s       zero_outdegree: %s" % (indentation, gnode_id, [p.get_id() for p in parent_nodes], [p.get_id() for p in  zero_outdegree_nodes]))
        return zero_outdegree_nodes

    def __is_node_runnable(self, node_id):
        print("Check node %s can be run" % node_id)
        in_edges = self.__graph.in_edges(node_id)
        for edge in in_edges:
            previous_node = edge[0]
            print("\tPrevious node %s status: %s" % (previous_node, self.__graph.nodes[previous_node]['data'].get_status()))
            if self.__graph.nodes[previous_node]['data'].get_status() != 'ended':
                return False
        return True

    def __run_step(self):
        for node_id in self.__running_nodes:

            node = self.__graph.nodes[node_id]['data']
            # if current node is ended search for next nodes
            if node.get_status() == 'ended':
                self.__running_nodes.remove(node_id)
                for out_edge in self.__graph.out_edges(node_id):
                    next_node_id = out_edge[1]
                    next_node = self.__graph.nodes[next_node_id]['data']

                    if self.__is_node_runnable(next_node_id):
                        print("Run node %s" % next_node_id)
                        self.__running_nodes.append(next_node_id)
                        if isinstance(next_node , PNode):
                            next_node.run()
            elif node.get_status() == 'failed':
                # just remove a failed node
                print("Failed node %s" % node_id)
                self.__running_nodes.remove(node_id)

    def run(self):
        if self.__running_statues != 'not_started':
            raise Exception("Already running")
        self.__running_statues = 'started'
        # loop over nodes
        while len(self.__running_nodes):
            self.__run_step()
        self.__running_statues = 'ended'

def main():
    aw = AnsibleWorkflow(workflow="input1.yml", inventory='inventory.ini')
    #aw.draw_graph()
    #aw.print_graph()
    aw.run_graphically()
    #aw.run()


if __name__ == "__main__":
    main()
