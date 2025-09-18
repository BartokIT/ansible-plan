import os
from graphviz import Digraph
from ansible_plan.core.engine import AnsibleWorkflow
from collections import defaultdict

def generate_graph(workflow: AnsibleWorkflow, output_path: str):
    """
    Generates a graph of the workflow with swimlanes.

    Args:
        workflow (AnsibleWorkflow): The workflow to visualize.
        output_path (str): The path to save the generated graph image.
    """
    dot = Digraph(comment='Ansible Workflow')
    dot.attr(compound='true')
    dot.attr('graph', rankdir='LR')

    # Group nodes by reference
    nodes_by_reference = defaultdict(list)
    for node_id in workflow.get_nodes():
        node = workflow.get_node_object(node_id)
        reference = "General"
        if hasattr(node, 'get_reference') and node.get_reference():
            reference = node.get_reference()
        nodes_by_reference[reference].append(node)

    # Create subgraphs for each reference (swimlane)
    for i, (reference, nodes) in enumerate(nodes_by_reference.items()):
        with dot.subgraph(name=f'cluster_{i}') as c:
            c.attr(label=reference)
            if len(nodes) == 1:
                # Add a hidden node to force the cluster to be drawn
                c.node(f'hidden_{i}', style='invis', width='0', height='0', label='')
            for node in nodes:
                c.node(node.get_id(), label=node.get_id(), shape='ellipse')

    # Add edges
    for from_node, to_node in workflow.get_graph().edges():
        dot.edge(from_node, to_node)

    # Render the graph
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    filename, format = os.path.splitext(output_path)
    if not format:
        format = "png"
    else:
        format = format[1:]

    dot.render(filename, format=format, view=False, cleanup=True)
