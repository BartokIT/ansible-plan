import typing
import warnings
import threading
from datetime import datetime
import time
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", message="networkx backend defined more than once: nx-loopback")
    import networkx as nx
import os
import logging
import logging.handlers
from .exceptions import AnsibleWorkflowDuplicateNodeId, AnsibleWorkflowPlaybookNodeCheck
from .models import WorkflowStatus, NodeStatus, Node, BNode, PNode, CNode, INode, WorkflowEventType, WorkflowEvent, WorkflowListener


class AnsibleWorkflow():
    def __init__(self, workflow_file, logging_dir, log_level, doubtful_mode: bool = False, filtered_nodes=None):
        self.__graph: nx.DiGraph = nx.DiGraph()
        self.__original_graph: nx.DiGraph = nx.DiGraph()
        self.__running_status = WorkflowStatus.NOT_STARTED
        self.__define_logger(logging_dir, log_level)
        self.__data = dict()
        self.__running_nodes = []
        self.__stopped = False
        self.__listeners: WorkflowListener = []
        self.__skipped_nodes: typing.List[str] = []
        self.__logging_dir = logging_dir
        self.__workflow_file = workflow_file
        self.__resume_event = threading.Event()
        self._validation_errors = []
        self.__pause_event = threading.Event()
        self.__pause_event.set()
        self.__stopping = False
        self.__doubtful_mode = doubtful_mode

    def get_validation_errors(self):
        return self._validation_errors

    def add_validation_error(self, error: str):
        self._validation_errors.append(error)

    def set_status(self, status: WorkflowStatus):
        self.__running_status = status

    def get_workflow_file(self):
        return self.__workflow_file

    def get_logging_dir(self):
        return self.__logging_dir

    def set_filtered_nodes(self, filter_nodes: typing.List[str]):
        if len(filter_nodes) > 0:
            remaining_nodes = set(self.__graph.nodes) - set(filter_nodes)
            self.__skipped_nodes = remaining_nodes

    def set_skipped_nodes(self, skipped_nodes: typing.List[str]):
        if len(skipped_nodes) > 0:
            self.__skipped_nodes = skipped_nodes

    def __define_logger(self, logging_dir, level):
        logger_name = self.__class__.__name__
        logger_file_path = os.path.join(logging_dir, 'workflow.log')
        if not os.path.exists(os.path.dirname(logger_file_path)):
            os.makedirs(os.path.dirname(logger_file_path))

        logger = logging.getLogger(logger_name)
        if level:
            logger.setLevel(getattr(logging, level.upper()))
        logger_handler = logging.handlers.TimedRotatingFileHandler(
            logger_file_path,
            when='d',
            backupCount=3,
            encoding='utf8'
        )
        logger_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
        logger.addHandler(logger_handler)
        self._logger = logger

    def add_event_listener(self, listener):
        self.__listeners.append(listener)

    def is_valid(self):
        for node_id in self.__graph.nodes:
            if isinstance(self.__data[node_id]['object'], PNode):
                try:
                    self.__data[node_id]['object'].check_node_input()
                except AnsibleWorkflowPlaybookNodeCheck as e:
                    self._validation_errors.append(str(e))

        # search cycle
        try:
            cycle = nx.find_cycle(self.__graph)
            error = "The workflow is cyclic: %s" % cycle
            self._logger.error(error)
            self._validation_errors.append(error)
        except nx.NetworkXNoCycle:
            pass

        if self._validation_errors:
            self._logger.error(
                "Impossible to run the workflow due to errors on some playbook"
            )
            return False
        return True

    def get_original_graph(self) -> nx.DiGraph:
        return self.__original_graph

    def get_original_graph_edges(self) -> typing.List[typing.List[str]]:
        return [[u, v] for u, v in self.__original_graph.edges()]

    def add_link(self, node_id: str, next_node_id: str):
        self.__graph.add_edge(node_id, next_node_id)

    def add_node(self, node: Node, other: dict = None):
        node.set_logger(self._logger)

        # check if the node already exists (is auto added if a link is added)
        if (self.is_node_present(node.get_id()) and node.get_id() in self.__data) or ',' in node.get_id():
            if node.get_id() in ['_s', '_e', '_root']:
                msg = "The node id %s name is reserved for internal purpose" % node.get_id()
            elif ',' in node.get_id():
                msg = "The node id %s contains unallowed characters ','" % node.get_id()
            else:
                msg = "Node id %s is already present" % node.get_id()
            self._logger.fatal(msg)
            raise AnsibleWorkflowDuplicateNodeId(msg)

        # store the id of the
        self.__graph.add_node(node.get_id())

        # attach datas to the graph node
        node_data = {}
        if other:
            node_data = other

        # attach an instance of the Node class to the graph node
        node_data.update(dict(object=node))
        self.__data[node.get_id()] = node_data

    def get_node_object(self, node_id: str) -> Node:
        return self.__data[node_id]['object']

    def notify_event(self, event_type: WorkflowEventType,
                     event: typing.Union[NodeStatus, WorkflowStatus],
                     content: typing.Any = None):
        event_obj = WorkflowEvent(event_type, event, content)
        self._logger.debug("Notifying TYPE: %s EVENT: %s CONTENT: %s" %
                           (event_type, event, content))

        for listener in self.__listeners:
            listener.notify_event(event_obj)

    def is_node_present(self, node_id: str):
        if node_id in self.__graph.nodes:
            return True
        return False

    def get_node(self, node_id: str) -> typing.List[typing.Any]:
        return self.__graph.nodes[node_id], self.__data[node_id]

    def get_nodes(self):
        return self.__graph.nodes

    def is_node_runnable(self, node_id):
        self._logger.debug("Check node %s can be run" % node_id)
        in_edges = self.__graph.in_edges(node_id)
        for edge in in_edges:
            previous_node = edge[0]
            self._logger.debug("\tPrevious node %s status: %s" % (previous_node, self.__data[previous_node]['object'].get_status()))
            if self.__data[previous_node]['object'].get_status() not in [NodeStatus.ENDED, NodeStatus.SKIPPED]:
                self._logger.debug("\tNot ended: %s" % previous_node)
                return False
        return True

    def is_running(self):
        return len(self.__running_nodes) != 0

    def get_running_status(self):
        return self.__running_status

    def get_running_nodes(self):
        return self.__running_nodes

    def get_graph(self):
        return self.__graph

    def get_node_datas(self):
        return self.__data

    def run_node(self, node_id):
        node = self.get_node_object(node_id)
        node.run()
        self._logger.info("Node: %s - %s - [ %s - ... ]" % (node, 'starting',
                            node.get_telemetry()["started"]))
        self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.RUNNING, node)

    def skip_node(self, node_id):
        node = self.get_node_object(node_id)
        self._logger.info("Node: %s - %s - [ %s - ... ]" % (node, 'skipped',
                            node.get_telemetry()["started"]))
        self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.SKIPPED, node)

    def add_running_node(self, node_id):
        self.__running_nodes.append(node_id)

    def is_stopping(self):
        return self.__stopped

    def stop(self, mode: str = 'graceful'):
        self._logger.info(f"Stop requested with mode: {mode}")
        self.__stopping = True
        self.__running_status = WorkflowStatus.STOPPING
        self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, f"Workflow stopping ({mode})")
        if mode == 'hard':
            for node_id in self.get_running_nodes():
                node = self.get_node_object(node_id)
                if isinstance(node, PNode):
                    node.stop()
        self.__pause_event.set()

    def pause(self):
        self._logger.info("Pausing workflow")
        self.__running_status = WorkflowStatus.PAUSED
        self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, "Workflow paused")
        self.__pause_event.clear()

    def resume(self):
        self._logger.info("Resuming workflow")
        self.__running_status = WorkflowStatus.RUNNING
        self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, "Workflow resumed")
        self.__pause_event.set()

    def _is_waiting_for_confirmation(self):
        for node_id in self.get_nodes():
            if self.get_node_object(node_id).get_status() == NodeStatus.AWAITING_CONFIRMATION:
                return True
        return False

    def get_some_failed_task(self):
        some_failed_tasks = False
        for node_id in self.get_nodes():
            if self.get_node_object(node_id).get_status() not in [NodeStatus.ENDED, NodeStatus.SKIPPED]:
                # print('--nodeid({}) KO {}'.format(node_id, self.get_node_object(node_id).get_status()))
                some_failed_tasks = True
            else:
                # print('--nodeid({}) ok {}'.format(node_id, self.get_node_object(node_id).get_status()))
                pass
        return some_failed_tasks

    def __run_step(self, end_node="_e"):
        self._logger.debug(f"__run_step: running_nodes={self.__running_nodes}")
        for node_id in list(self.__running_nodes):
            node = self.get_node_object(node_id)
            status = node.get_status()
            self._logger.debug(f"__run_step: processing node {node_id} with status {status}")
            # if current node is ended search for next nodes
            if isinstance(node, CNode) and status == NodeStatus.RUNNING:
                node.set_status(NodeStatus.ENDED)
            elif isinstance(node, (BNode, INode)) and status == NodeStatus.NOT_STARTED:
                node.set_status(NodeStatus.ENDED)
                self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.ENDED, node)
            elif status in [NodeStatus.ENDED, NodeStatus.SKIPPED]:
                self._logger.info(f"Node {node_id} finished with status {status}. Setting end time.")
                self.__running_nodes.remove(node_id)
                if not node.is_skipped():
                    node.set_ended_time(datetime.now())
                    self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.ENDED, node)

                for out_edge in self.__graph.out_edges(node_id):
                    next_node_id = out_edge[1]
                    next_node = self.get_node_object(next_node_id)

                    # check if a node as previous nodes ended and not already started
                    if self.is_node_runnable(next_node_id) and next_node_id not in self.__running_nodes:
                        if next_node_id != end_node and not self.__stopping:
                            self.__running_nodes.append(next_node_id)
                            if isinstance(next_node, PNode):
                                # run a node
                                self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.PRE_RUNNING, next_node)
                                if self.__doubtful_mode:
                                    next_node.set_status(NodeStatus.AWAITING_CONFIRMATION)
                                    self.notify_event(
                                        WorkflowEventType.NODE_EVENT,
                                        NodeStatus.AWAITING_CONFIRMATION,
                                        next_node
                                    )
                                elif not next_node.is_skipped():
                                    self.run_node(next_node_id)
                                else:
                                    self.skip_node(next_node_id)
                            elif isinstance(next_node, CNode):
                                next_node.set_status(NodeStatus.AWAITING_CONFIRMATION)
                                self.notify_event(
                                    WorkflowEventType.NODE_EVENT,
                                    NodeStatus.AWAITING_CONFIRMATION,
                                    next_node
                                )


            elif status == NodeStatus.AWAITING_CONFIRMATION:
                self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.AWAITING_CONFIRMATION, node)
                self.__running_nodes.remove(node_id)
            elif status == NodeStatus.STOPPED:
                self._logger.info(f"Node {node_id} stopped. Setting end time.")
                node.set_ended_time(datetime.now())
                self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.STOPPED, node)
                self.__running_nodes.remove(node_id)
            elif status == NodeStatus.FAILED:
                # just remove a failed node
                # print("Failed node %s" % node_id)
                node.set_ended_time(datetime.now())
                self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.FAILED, node)
                self.__running_nodes.remove(node_id)
                # Do not set workflow status to FAILED here, to allow for retry.

            if isinstance(node, PNode) and node.get_status() in ['ended', 'failed', 'skipped']:
                self._logger.info("Node: %s - %s - [ %s - %s]" % (node_id, node.get_status(),
                                  node.get_telemetry()['started'], node.get_telemetry()['ended']))

    def _set_skipped_nodes(self, start_node: str, end_node: str):
        '''
        Set the nodes to be skipped taking start node and end nodes into account and
        also filtered nodes.
        Args:
            start_node (string): The identifier of the starting node for the graph
            end_node (string): The identifier of the ending node for the graph
        '''
        # set skipped nodes from filtered nodes
        for node in self.__skipped_nodes:
            self.get_node_object(node).set_skipped()
        # skipped from start
        self._logger.info("Setting skipped %s" % self.__graph.in_edges(start_node))
        skipped_from_start = [s for s, _ in self.__graph.in_edges(start_node)]
        while len(skipped_from_start) > 0:
            actual_node_id = skipped_from_start.pop()
            actual_node = self.get_node_object(actual_node_id)
            actual_node.set_skipped()
            for prev, _ in self.__graph.in_edges(actual_node.get_id()):
                skipped_from_start.append(prev)

        skipped_after_end = [e for _, e in self.__graph.out_edges(end_node)]
        while len(skipped_after_end) > 0:
            actual_node_id = skipped_after_end.pop()
            actual_node = self.get_node_object(actual_node_id)
            actual_node.set_skipped()
            for _, next in self.__graph.out_edges(actual_node.get_id()):
                skipped_after_end.append(next)

    def restart_failed_node(self, node_id: str):
        node = self.get_node_object(node_id)
        if not node or node.get_status() != NodeStatus.FAILED:
            self._logger.warning(f"Node {node_id} cannot be restarted.")
            return

        self._logger.info(f"Restarting node {node_id}")

        # Set status back to RUNNING
        self.__running_status = WorkflowStatus.RUNNING
        self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, f"Workflow resuming from node {node_id}")

        # Reset node status
        node.reset_status()

        self.run_node(node_id)
        self.add_running_node(node_id)
        self.__resume_event.set()

    def skip_failed_node(self, node_id: str):
        node = self.get_node_object(node_id)
        if not node or node.get_status() != NodeStatus.FAILED:
            self._logger.warning(f"Node {node_id} cannot be skipped as it is not in FAILED state.")
            return

        self._logger.info(f"Skipping failed node {node_id}")

        # Set status back to RUNNING
        self.__running_status = WorkflowStatus.RUNNING
        self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, f"Workflow resuming, skipping node {node_id}")

        # Set node as skipped
        node.set_skipped()
        self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.SKIPPED, node)

        # Add node to running nodes so its successors can be processed
        self.add_running_node(node_id)
        self.__resume_event.set()

    def approve_node(self, node_id: str):
        node = self.get_node_object(node_id)
        if not node or node.get_status() != NodeStatus.AWAITING_CONFIRMATION:
            self._logger.warning(f"Node {node_id} cannot be approved.")
            return

        self._logger.info(f"Approving node {node_id}")
        node.set_status(None)
        if isinstance(node, CNode):
            node.set_status(NodeStatus.RUNNING)
            self.add_running_node(node_id)
        else:
            self.run_node(node_id)

    def disapprove_node(self, node_id: str):
        node = self.get_node_object(node_id)
        if not node or node.get_status() != NodeStatus.AWAITING_CONFIRMATION:
            self._logger.warning(f"Node {node_id} cannot be disapproved.")
            return

        self._logger.info(f"Disapproving node {node_id}")
        node.set_status(NodeStatus.FAILED)
        self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.FAILED, node)


    def run(self, start_node: str = "_s", end_node: str = "_e", verify_only: bool = False):
        '''
        Run the workflows starting from a graph node until reaching the end node.
        Args:
            start_node (string): The identifier of the starting node for the graph
            end_node (string): The identifier of the ending node for the graph
            verify_only (bool): A flag that skip the workflow run and verify only the correctness

        '''

        # perform validation of the
        if not self.is_valid():
            self.__running_status = WorkflowStatus.FAILED
            self.notify_event(
                WorkflowEventType.WORKFLOW_EVENT,
                self.__running_status,
                self._validation_errors,
            )
            return

        if verify_only:
            self.__running_status = WorkflowStatus.ENDED
            return

        if self.__running_status != WorkflowStatus.NOT_STARTED:
            # This allows to re-enter the loop on retry
            if self.__running_status == WorkflowStatus.RUNNING:
                return
            raise Exception("Already running")

        # check the starting node
        self._logger.info("Start from node %s" % start_node)
        if not self.is_node_present(start_node):
            error = "Starting node not exist: %s" % start_node
            self._logger.error("Starting node not exist: %s" % start_node)
            self.__running_status = WorkflowStatus.FAILED
            self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, error)
            return

        self._set_skipped_nodes(start_node, end_node)
        start_node_object = self.get_node_object(start_node)
        self.__running_status = WorkflowStatus.RUNNING
        self.add_running_node(start_node)
        self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, start_node)

        if isinstance(start_node_object, PNode):
            self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.RUNNING, start_node_object)
            self.run_node(start_node)

        # loop over nodes
        while not self.__stopped:
            self.__pause_event.wait()
            self.__run_step(end_node)

            if not self.is_running():
                if self.__stopping:
                    break

                if self._is_waiting_for_confirmation():
                    if self.__running_status != WorkflowStatus.PAUSED:
                        self.__running_status = WorkflowStatus.PAUSED
                        self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, 'Workflow paused, waiting for confirmation.')
                    time.sleep(0.5) # Prevent busy-waiting
                    continue

                if self.get_some_failed_task():
                    # There are failed tasks, set status and wait for user to retry
                    self.__running_status = WorkflowStatus.FAILED
                    self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, 'Workflow failed, waiting for retry.')
                    self.__resume_event.clear()
                    self.__resume_event.wait(timeout=1)
                else:
                    # No running nodes and no failed nodes, we are done
                    self.__running_status = WorkflowStatus.ENDED
                    self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, end_node)
                    break

            time.sleep(0.2)

        if self.__stopping and self.__running_status != WorkflowStatus.ENDED:
            self.__running_status = WorkflowStatus.FAILED
            self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, "Workflow stopped")
