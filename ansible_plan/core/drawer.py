import graphviz
import os
import logging
from .models import PNode, CNode, INode, BNode

logger = logging.getLogger(__name__)

def generate_workflow_svg(workflow, output_path_prefix):
    '''
    Generate an SVG image of the workflow graph.
    Args:
        workflow (AnsibleWorkflow): The workflow instance.
        output_path_prefix (str): The path prefix where to save the SVG (e.g. /path/to/workflow).
    '''
    try:
        dot = graphviz.Digraph(name='workflow', format='svg')

        # Global attributes
        dot.attr(rankdir='LR', compound='true', bgcolor='#0d2c4b', fontcolor='white')
        dot.attr('node', shape='rect', style='filled,rounded', color='white', fontcolor='white', fillcolor='#1e4a6e', fontname='Arial')
        dot.attr('edge', color='white', fontcolor='white', fontname='Arial')

        for node_id in workflow.get_nodes():
            if node_id == '_root':
                continue

            node_obj = workflow.get_node_object(node_id)
            label = node_id

            # Truncate label if too long
            if len(label) > 40:
                label = label[:37] + "..."

            if node_id == '_s':
                fillcolor = "#4caf50"
            elif node_id == '_e':
                fillcolor = "#f44336"
            else:
                fillcolor = '#1e4a6e'

            if isinstance(node_obj, BNode):
                # Round (ellipse)
                dot.node(node_id, label, shape='ellipse', fillcolor=fillcolor)
            elif isinstance(node_obj, PNode):
                # Rounded rectangle
                dot.node(node_id, label, shape='rect', style='filled,rounded', fillcolor=fillcolor)
            elif isinstance(node_obj, CNode):
                # Diamond
                dot.node(node_id, label, shape='diamond', fillcolor=fillcolor, height='1', width='1')
            elif isinstance(node_obj, INode):
                # Square rectangle (just filled, no rounded)
                dot.node(node_id, label, shape='rect', style='filled', fillcolor=fillcolor)
            else:
                # Fallback
                dot.node(node_id, label, fillcolor=fillcolor)

        # Edges
        # We use the execution graph for visualization as it represents the logical flow
        # between tasks and blocks.
        graph = workflow.get_graph()
        for u, v in graph.edges():
            # We hide the _root node as it is used for internal hierarchy
            if u == '_root' or v == '_root':
                continue
            dot.edge(u, v)

        # Save
        dot.render(output_path_prefix, cleanup=True)
        logger.info(f"Workflow SVG generated at {output_path_prefix}.svg")
    except Exception as e:
        logger.error(f"Failed to generate workflow SVG: {e}")
