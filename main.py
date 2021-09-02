#!/bin/env python3
import ansible_runner
import networkx as nx
import yaml
import matplotlib.pyplot as plt

class Node():
    def __init__(self, id):
        self.__id = id

    def get_id(self):
        return self.__id

    def __eq__(self, other):
        return self.__id == other.get_id()

    def __hash__(self):
        return hash(self.__id)

class BNode(Node):
    pass

class PNode(Node):
    def __init__(self, id, playbook):
        super(PNode, self).__init__(id)
        self.__playbook = playbook


class AnsibleWorkflow():
    def __init__(self):
        self.__graph = nx.DiGraph()
        self.__frontier = []
        self.__allowed_node_keys = set(['block', 'import_playbook', 'name', 'strategy'])

    def _get_input(self, filename):
        with open(filename, 'r') as stream:
            data_loaded = yaml.safe_load(stream)
        return data_loaded

    def import_file(self, filename):
        root_node=BNode('r')
        self.__graph.add_node('r', data=root_node)
        self.__frontier.append(root_node)
        self._import_nodes(self._get_input(filename), None)
        nx.draw_spring(self.__graph, with_labels=True)
        print("%s" % list(self.__graph))
        plt.savefig("path.png")

    def _check_node_syntax(self, node):
        remaining_keys = set(node.keys()) - self.__allowed_node_keys
        if remaining_keys:
            raise Exception("The following keys are not allowed %s" % remaining_keys)

    def _import_nodes(self, to_be_imported, parent_node, strategy='serial'):

        # variable initialization
        parent_node_id =  parent_node.get_id() if parent_node is not None else ''
        imported_pnodes = []
        print("strategy: %s, parent_node_id: %s, frontier:  %s" % (strategy, parent_node_id, [p.get_id() for p in self.__frontier]))
        # init to loop over the structure
        i=1
        for inode in to_be_imported:
            # basic syntax check of structure's node
            self._check_node_syntax(inode)

            # generate a node identifier for the graph
            gnode_id= (parent_node_id + '.' + str(i)).lstrip('.')


            # generate the object representing the graph
            if 'block' in inode:
                gnode = BNode(gnode_id)
            else:
                gnode = PNode(gnode_id, inode['import_playbook'])
                self.__frontier.append(gnode)

            # if the strategy is serial then the nodes part of the frontier need to be added as an edge to the current node
            if strategy == 'serial':
                for fnode in self.__frontier:
                    self.__graph.add_edge(fnode.get_id(), gnode_id)
                self.__frontier = []

            # if the node is a block node then we need to import the subnodes
            if 'block' in inode:
                imported_sub_pnodes = self._import_nodes(inode['block'], gnode, inode.get('strategy','parallel'))
            else:
                imported_sub_pnodes = []


            # the node specification is added
            self.__graph.add_node(gnode_id, data=gnode)

            # the parent node is linked with the son
            if parent_node is not None:
                self.__graph.add_edge(parent_node_id, gnode_id)

            #if strategy == 'parallel':
            #self.__frontier.extend(imported_sub_pnodes)
            print("\t node: %s, strategy: %s,  imported_sub_pnodes: %s, frontier: %s" % (gnode_id, strategy, [p.get_id() for p in imported_sub_pnodes], [p.get_id() for p in self.__frontier]) )
            i=i+1
        return imported_pnodes


def main():
    aw = AnsibleWorkflow()
    i = aw.import_file("../examples/input1.yml")


if __name__ == "__main__":
    main()
