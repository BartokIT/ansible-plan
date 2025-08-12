import requests
import time
import threading
import networkx as nx
from .workflow import WorkflowStatus, NodeStatus, WorkflowEvent, WorkflowEventType, Node, BNode, PNode
from .exceptions import ExitCodes

class ProxyNode(Node):
    """A proxy for a node in the backend's workflow."""
    def __init__(self, id, data):
        super().__init__(id)
        self._data = data
        self._status = NodeStatus(data.get('status', 'not_started'))
        self._type = data.get('type', 'unknown')

    def get_status(self):
        return self._status

    def get_type(self):
        return self._type

    def get_telemetry(self):
        return self._data.get('telemetry', {})

    def get_playbook(self):
        return self._data.get('playbook', 'N/A')

    def get_description(self):
        return self._data.get('description', 'N/A')

    def get_reference(self):
        return self._data.get('reference', 'N/A')

class FrontendWorkflowProxy:
    def __init__(self, backend_url, logging_dir):
        self._backend_url = backend_url
        self._logging_dir = logging_dir
        self._listeners = []
        self._last_state = {}
        self._running = False
        self._status = WorkflowStatus.NOT_STARTED
        self._nodes_data = {}
        self._graph = nx.DiGraph()

    def add_event_listener(self, listener):
        self._listeners.append(listener)

    def notify_event(self, event_type, event, content):
        event_obj = WorkflowEvent(event_type, event, content)
        for listener in self._listeners:
            listener.notify_event(event_obj)

    def get_logging_dir(self):
        return self._logging_dir

    def get_running_status(self):
        return self._status

    def get_node_datas(self):
        # The output classes expect a dict of dicts with an 'object' key
        # pointing to a node object.
        datas = {}
        for node_id, node_data in self._nodes_data.items():
            datas[node_id] = {'object': self.get_node_object(node_id)}
        return datas

    def get_nodes(self):
        return self._nodes_data.keys()

    def get_node_object(self, node_id):
        if node_id in self._nodes_data:
            return ProxyNode(node_id, self._nodes_data[node_id])
        return None

    def get_node(self, node_id):
        if node_id in self._graph:
            graph_node_attrs = self._graph.nodes[node_id]
        else:
            graph_node_attrs = {}

        if node_id in self._nodes_data:
            node_data = {'object': self.get_node_object(node_id)}
        else:
            node_data = {}

        return graph_node_attrs, node_data

    def get_graph(self):
        return self._graph

    def get_original_graph(self):
        return self.get_graph()

    def is_stopping(self):
        return False

    def stop(self):
        try:
            requests.delete(f"{self._backend_url}/workflow")
        except requests.exceptions.RequestException as e:
            print(f"Error stopping workflow: {e}")

    def fetch_graph(self):
        try:
            response = requests.get(f"{self._backend_url}/workflow/graph")
            if response.status_code == 200:
                graph_data = response.json()
                self._graph = nx.readwrite.json_graph.node_link_graph(graph_data)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching workflow graph: {e}")

    def _update_state_and_notify(self, new_state):
        old_nodes_data = self._last_state.get('nodes', {})
        new_nodes_data = new_state.get('nodes', {})

        # Update internal state first
        self._status = WorkflowStatus(new_state.get('status', 'not_started'))
        self._nodes_data = new_nodes_data

        for node_id, new_node_data in new_nodes_data.items():
            old_node_data = old_nodes_data.get(node_id, {})

            old_status_str = old_node_data.get('status')
            new_status_str = new_node_data.get('status')

            if new_status_str != old_status_str:
                new_status_enum = NodeStatus(new_status_str)
                node_proxy_obj = self.get_node_object(node_id)
                self.notify_event(WorkflowEventType.NODE_EVENT, new_status_enum, node_proxy_obj)

        self._last_state = new_state

    def run(self, start_node, end_node, verify_only):
        self._running = True
        self.notify_event(WorkflowEventType.WORKFLOW_EVENT, WorkflowStatus.RUNNING, "Workflow started")

        while self._running:
            try:
                response = requests.get(f"{self._backend_url}/workflow")
                if response.status_code == 200:
                    new_state = response.json()
                    self._update_state_and_notify(new_state)

                    if self._status in [WorkflowStatus.ENDED, WorkflowStatus.FAILED]:
                        self._running = False
                        self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self._status, "Workflow finished")

                elif response.status_code == 404:
                    self._running = False
                    if self._status not in [WorkflowStatus.ENDED, WorkflowStatus.FAILED]:
                        self._status = WorkflowStatus.ENDED
                        self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self._status, "Workflow finished")

                time.sleep(1) # Polling interval
            except requests.exceptions.RequestException as e:
                print(f"Error polling workflow status: {e}")
                self._running = False
                self.notify_event(WorkflowEventType.WORKFLOW_EVENT, WorkflowStatus.FAILED, "Connection error")
