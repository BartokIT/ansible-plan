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

        nodes_by_reference = {}

        for node_id in workflow.get_nodes():
            if node_id == '_root':
                continue

            node_obj = workflow.get_node_object(node_id)
            ref = node_obj.get_reference() or "Default"

            if ref not in nodes_by_reference:
                nodes_by_reference[ref] = []
            nodes_by_reference[ref].append(node_id)

        # Palette inspired by the image
        palette = {
            "CUSTOMER": "#f06292",
            "SALES": "#ffa726",
            "STOCKS": "#42a5f5",
            "FINANCE": "#26c6da",
            "DEFAULT": "#90a4ae"
        }

        # Order lanes: put Default last
        sorted_refs = sorted(nodes_by_reference.keys(), key=lambda x: (x == "Default", x))

        for ref in sorted_refs:
            lane_color = palette.get(ref.upper(), palette["DEFAULT"])
            with dot.subgraph(name=f'cluster_{ref}') as c:
                c.attr(label=ref, color=lane_color, fontcolor=lane_color, style='dashed')
                for node_id in nodes_by_reference[ref]:
                    node_obj = workflow.get_node_object(node_id)
                    description = node_obj.get_description()

                    if node_id == '_s':
                        label = "START"
                        fillcolor = "#4caf50"
                    elif node_id == '_e':
                        label = "FINISH"
                        fillcolor = "#f44336"
                    else:
                        label = description if description else node_id
                        fillcolor = '#1e4a6e'

                    # Truncate label if too long
                    if len(label) > 40:
                        label = label[:37] + "..."

                    shape = 'rect'
                    if isinstance(node_obj, CNode):
                        shape = 'diamond'
                        # For diamonds, we might want a slightly different style
                        c.node(node_id, label, shape=shape, fillcolor=fillcolor, height='1', width='1')
                    else:
                        c.node(node_id, label, shape=shape, fillcolor=fillcolor)

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
