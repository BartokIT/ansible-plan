#!/bin/env python3
import ansible_runner
import networkx as nx
import yaml
import matplotlib.pyplot as plt
import random 
import string 
import inspect

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
        self.__allowed_node_keys = set(['block', 'import_playbook', 'name', 'strategy', 'id'])

    def _get_input(self, filename):
        with open(filename, 'r') as stream:
            data_loaded = yaml.safe_load(stream)
        return data_loaded

    def import_file(self, filename):
        root_node=BNode('r')
        self.__graph.add_node('r', data=root_node)
        #self.__frontier.append(root_node)
        self._import_nodes(self._get_input(filename), [root_node,],  strategy='serial')
        pos=nx.nx_agraph.graphviz_layout(self.__graph, prog="dot")
        nx.draw(self.__graph, pos=pos)
        nx.draw_networkx_labels(self.__graph, pos=pos)
        print("%s" % list(self.__graph))
        plt.savefig("path.png")

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
            
            # generate a node identifier
            gnode_id= inode.get('id',''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5)))
            print("%s node: %s       parents: %s " % (indentation, gnode_id, [p.get_id() for p in parent_nodes]))
            for parent_node in parent_nodes:
                self.__graph.add_edge(parent_node.get_id(), gnode_id)

            if strategy == 'serial':
                parent_nodes = []

            # generate the object representing the graph
            if 'block' in inode:
                gnode = BNode(gnode_id)
                block_sub_nodes = self._import_nodes(inode['block'], [gnode,], inode.get('strategy','parallel'))
            else:
                gnode = PNode(gnode_id, inode['import_playbook'])

            # the node specification is added
            self.__graph.add_node(gnode_id, data=gnode)


            if 'block' not in inode:
                if strategy == 'parallel' or (strategy == 'serial' and inode == to_be_imported[-1]):
                    zero_outdegree_nodes.append(gnode)

            # if the strategy is serial
            if strategy == 'serial':
                if 'block' in inode:
                    # add the node from the subtree as parent
                    parent_nodes = block_sub_nodes
                else:
                    # or add current node as the parent
                    parent_nodes = [gnode,]
            
        return zero_outdegree_nodes


def main():
    aw = AnsibleWorkflow()
    i = aw.import_file("../examples/input3.yml")


if __name__ == "__main__":
    main()
