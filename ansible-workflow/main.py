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
    
    def get_playbook(self):
        return self.__playbook

def nudge(pos, x_shift, y_shift):
    return {n:(x + x_shift, y + y_shift) for n,(x,y) in pos.items()}

class AnsibleWorkflow():
    def __init__(self, workflow_file):
        self.__graph = nx.DiGraph()
        self.__frontier = []
        self.__allowed_node_keys = set(['block', 'import_playbook', 'name', 'strategy', 'id'])
        self.__workflow_filename = workflow_file
        self.__import_file(self.__workflow_filename)

    def _get_input(self, filename):
        with open(filename, 'r') as stream:
            data_loaded = yaml.safe_load(stream)
        return data_loaded

    def __import_file(self, filename):
        input_file_data = self._get_input(filename)
        input_file_data.insert(0, dict(id='s',block=[]))
        input_file_data.append(dict(id='e',block=[]))
        self._import_nodes(input_file_data, [], strategy='serial')


    def draw_graph(self):
        pos=nx.nx_agraph.graphviz_layout(self.__graph)        
        nx.draw(self.__graph, pos=pos, verticalalignment='top')
        
        # draw labels
        label_position = nudge(pos, 0, 15) 
        labels = {n: '' if isinstance(d['data'], BNode) else d['data'].get_playbook() for n, d in  list(self.__graph.nodes(data=True)) }
        nx.draw_networkx_labels(self.__graph,labels=labels, pos=label_position, font_size=10)
        
        # draw block nodes differently
        block_nodes=[n for n, d in  list(self.__graph.nodes(data=True)) if isinstance(d['data'], BNode) and d['data'].get_id() != 's' and d['data'].get_id() != 'e']        
        nx.draw_networkx_nodes(self.__graph, pos, nodelist=block_nodes, node_size=350, node_color="#777")
        
        # draw start and end node differently
        nx.draw_networkx_nodes(self.__graph, pos, nodelist=['s'], node_size=500, node_color="#580")
        nx.draw_networkx_nodes(self.__graph, pos, nodelist=['e'], node_size=500, node_color="#a10")

        plt.savefig(self.__workflow_filename + '.png')

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
            print("-->> %s node: %s       parents: %s       zero_outdegree: %s" % (indentation, gnode_id, [p.get_id() for p in parent_nodes], [p.get_id() for p in  zero_outdegree_nodes]))
            
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
                gnode = PNode(gnode_id, inode['import_playbook'])

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


def main():
    aw = AnsibleWorkflow("../examples/input4.yml")
    aw.draw_graph()

if __name__ == "__main__":
    main()
