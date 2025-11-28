# -*- coding: utf-8 -*-
#
# Copyright 2021 BartokIT
# Ansible® is a registered trademark of Red Hat, Inc. in the United States and other countries.
#
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License v3.0 as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the  GNU Affero General Public License v3.0
# along with this program; if not, see <https://www.gnu.org/licenses/agpl.html/>.

from diagrams import Diagram
from diagrams.onprem.iac import Ansible
from diagrams.aws.general import User
from diagrams.general.blank import Info
from diagrams.aws.management import ControlTower

from ansible_plan.core.models import PNode, INode, CNode, BNode


def draw_graph(workflow, out_file):
    graph_name = f"Ansible Workflow: {workflow.get_workflow_file()}"
    with Diagram(graph_name, show=False, filename=out_file, direction="TB"):
        nodes = {}
        for node_id in workflow.get_nodes():
            if node_id in ['_s', '_e', '_root']:
                continue

            node_obj = workflow.get_node_object(node_id)
            label = f"{node_id}"

            if isinstance(node_obj, PNode):
                description = node_obj.get_description()
                if description:
                    label += f"\\n({description})"
                nodes[node_id] = Ansible(label)
            elif isinstance(node_obj, INode):
                description = node_obj.get_description()
                if description:
                    label = f"{description}"
                nodes[node_id] = Info(label)
            elif isinstance(node_obj, CNode):
                description = node_obj.get_description()
                if description:
                    label += f"\\n({description})"
                nodes[node_id] = User(label)
            elif isinstance(node_obj, BNode):
                nodes[node_id] = ControlTower(f"Block: {node_id}")

        for u, v in workflow.get_original_graph_edges():
            if u in nodes and v in nodes:
                nodes[u] >> nodes[v]
