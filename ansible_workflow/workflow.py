from enum import Enum
import sys
import typing
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", message="networkx backend defined more than once: nx-loopback")
    import networkx as nx
import ansible_runner
import abc
import os
import os.path
import time
import logging
import logging.handlers
from datetime import datetime
from .exceptions import AnsibleWorkflowDuplicateNodeId, ExitCodes


class WorkflowStatus(Enum):
    """ Define the character for the application"""
    NOT_STARTED = 'not_started'
    RUNNING = 'running'
    ENDED = 'ended'
    FAILED = 'failed'


class NodeStatus(Enum):
    """ Define the character for the application"""
    RUNNING = 'running'
    PRE_RUNNING = 'pre_running'
    ENDED = 'ended'
    FAILED = 'failed'
    SKIPPED = 'skipped'
    NOT_STARTED = 'not_started'


class Node():
    ''' An abstract Node class of the graph'''
    __metaclass__ = abc.ABCMeta

    def __init__(self, id: str):
        '''
        Initialize a generic node of the graph
        Args:
            id (str): The unique identifier of the node inside the workflows
        Raises:
            AnsibleWorkflowVaultScriptNotSet: If a node specify some vault ids but the vault script is not set
        '''
        self.__id = id
        self._logger: logging.Logger = logging
        self._started_time: datetime = None
        self._ended_time: datetime = None
        self.__skipped = False

    def get_id(self) -> str:
        return self.__id

    def set_logger(self, logger: logging.Logger):
        self._logger = logger

    def set_skipped(self):
        self.__skipped = True

    def is_skipped(self):
        return self.__skipped

    def __eq__(self, other):
        return self.__id == other.get_id()

    def __hash__(self):
        return hash(self.__id)

    def __str__(self):
        return "%s[%s]" % (self.get_type(), self.get_id())

    @abc.abstractmethod
    def get_status(self):
        return NodeStatus.ENDED

    @abc.abstractmethod
    def get_type(self):
        pass

    def set_ended_time(self, time):
        self._ended_time = time

    def set_started_time(self, time):
        self._started_time = time

    def get_telemetry(self):
        return dict(started=self._started_time.strftime("%H:%M:%S") if self._started_time else '',
                    ended=self._ended_time.strftime("%H:%M:%S") if self._ended_time else '')


class BNode(Node):
    def get_status(self):
        return NodeStatus.ENDED

    def get_type(self):
        return 'block'


class PNode(Node):
    def __init__(self, id, playbook, inventory, artifact_dir, limit=None, project_path=None, extra_vars={}, vault_ids=[], check_mode=False, diff_mode=True, verbosity=1, description='N/A', reference='N/A'):
        super(PNode, self).__init__(id)
        self.__playbook = playbook
        self.__inventory = inventory
        self.__extravars = extra_vars
        self.__artifact_dir = artifact_dir
        self.__limit = limit
        self.__vault_ids = vault_ids
        self.__project_path = project_path
        self.__thread = None
        self.__runner = None
        self.__check_mode = check_mode
        self.__diff_mode = diff_mode
        self.__verbosity = verbosity
        self.__description = description
        self.__reference = reference

    def check_node_input(self):
        valid = True
        # convert project path in absolute path
        if not os.path.isabs(self.__project_path):
            self.__project_path = os.path.abspath(self.__project_path)

        if not os.path.exists(self.__project_path):
            self._logger.error("Node %s project path %s doesn't exists" % (self.get_id(), self.__project_path))
            valid = False

        if self.__inventory is None:
            self._logger.error("Node %s inventory not set" % self.get_id())
            valid = False
        elif not os.path.exists(self.__inventory):
            self._logger.error("Node %s inventory doesn't exists: %s" % (self.get_id(), self.__inventory))
            valid = False
        else:
            self.__inventory = os.path.abspath(self.__inventory)

        if self.__project_path and not os.path.isabs(self.__playbook):
            self.__playbook = os.path.join(self.__project_path, self.__playbook)

        if not os.path.exists(self.__playbook):
            self._logger.error("Node %s playbook doesn't exists: %s" % (self.get_id(), self.__playbook))
            valid = False
        return valid

    def get_verbosity(self):
        return self.__verbosity

    def set_verbosity(self, verbosity):
        self.__verbosity = verbosity

    def get_status(self):
        if self.is_skipped():
            return NodeStatus.SKIPPED
        elif self.__thread is None:
            return NodeStatus.NOT_STARTED
        else:
            # print("Node %s status is %s - error is %s" % (self.get_id(), self.__runner.errored, self.__runner.status ))
            if self.__thread.is_alive():
                return NodeStatus.RUNNING
            elif self.is_failed():
                return NodeStatus.FAILED
            else:
                return NodeStatus.ENDED

    def get_type(self):
        return 'playbook'

    def is_failed(self):
        return self.__runner.status == 'failed'

    def get_playbook(self):
        return self.__playbook

    def get_description(self):
        return self.__description
    def get_reference(self):
        return self.__reference

    def run(self):
        self.set_started_time(datetime.now())
        self.__inventory = os.path.abspath(self.__inventory)
        self.__playbook = os.path.abspath(self.__playbook)

        # put the current directory to the parent of the playbook
        env_vars = {}

        if self.__project_path:
            env_vars = {'ANSIBLE_COLLECTIONS_PATHS': os.path.join(self.__project_path, 'collections')}

        playbook_cmd_line = ' '.join(["--vault-id %s" % vid for vid in self.__vault_ids])
        if self.__check_mode:
            playbook_cmd_line += ' --check'
        if self.__diff_mode:
            playbook_cmd_line += ' --diff'

        # modify identification in case of multiple start
        ident = self.get_id()
        if os.path.exists(os.path.join(self.__artifact_dir, "%s" % self.get_id())):
            i=1
            ident = "%s_%s" % (self.get_id(), i)
            while os.path.exists(os.path.join(self.__artifact_dir, "%s_%s" % (self.get_id(), i))):
                i = i + 1
                ident = "%s_%s" % (self.get_id(), i)
        self.ident = ident
        self.__thread, self.__runner = ansible_runner.run_async(playbook=self.__playbook,
                                                                inventory=self.__inventory,
                                                                ident=ident,
                                                                limit=self.__limit,
                                                                project_dir=self.__project_path,
                                                                event_handler=lambda x: False,
                                                                omit_event_data=True,
                                                                envvars=env_vars,
                                                                verbosity=self.get_verbosity(),
                                                                artifact_dir=self.__artifact_dir,
                                                                settings={
                                                                    'suppress_ansible_output': True
                                                                },
                                                                # vault_ids=self.__vault_ids,
                                                                cmdline=playbook_cmd_line,
                                                                extravars=self.__extravars,
                                                                quiet=True)


class WorkflowEventType(Enum):
    """ Define the return codes for the application"""
    NODE_EVENT = 1
    WORKFLOW_EVENT = 2


class WorkflowEvent(object):
    def __init__(self, event_type: WorkflowEventType,
                 event: typing.Union[NodeStatus, WorkflowStatus],
                 content: typing.Any = None):
        self._type = event_type
        self._event = event
        self._content = content

    def get_type(self) -> WorkflowEventType:
        return self._type

    def get_event(self):
        return self._event, self._content

    def __str__(self):
        return '%s -> %s: %s' % (self._type.name, self._event, self._content)


class WorkflowListener(object):
    @abc.abstractmethod
    def notify_event(self, event: WorkflowEvent):
        ''' A notification method to be overwrited'''
        pass


class AnsibleWorkflow():
    def __init__(self, logging_dir, log_level, filtered_nodes=None):
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
        valid = True
        for node_id in self.__graph.nodes:
            if isinstance(self.__data[node_id]['object'], PNode):
                if not self.__data[node_id]['object'].check_node_input():
                    valid = False
        if not valid:
            self._logger.error("Impossible to run the workflow due to errors on some playbook")

        # search cycle
        cycles_in_graph = True
        try:
            nx.find_cycle(self.__graph)
        except nx.NetworkXNoCycle:
            cycles_in_graph = False

        if cycles_in_graph:
            self._logger.error("The workflow is cyclic")
            valid = False
        return valid

    def get_original_graph(self) -> nx.DiGraph:
        return self.__original_graph

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

    def stop(self):
        self.__stopped = True

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
        for node_id in self.__running_nodes:
            node = self.get_node_object(node_id)
            # if current node is ended search for next nodes
            if node.get_status() in [NodeStatus.ENDED, NodeStatus.SKIPPED]:
                self.__running_nodes.remove(node_id)
                if not node.is_skipped():
                    node.set_ended_time(datetime.now())
                    self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.ENDED, node)

                for out_edge in self.__graph.out_edges(node_id):
                    next_node_id = out_edge[1]
                    next_node = self.get_node_object(next_node_id)

                    # check if a node as previous nodes ended and not already started
                    if self.is_node_runnable(next_node_id) and next_node_id not in self.__running_nodes:
                        if next_node_id != end_node and not self.__stopped:
                            self.__running_nodes.append(next_node_id)
                            if isinstance(next_node, PNode):
                                # run a node
                                self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.PRE_RUNNING, next_node)
                                if not next_node.is_skipped():
                                    self.run_node(next_node_id)
                                else:
                                    self.skip_node(next_node_id)


            elif node.get_status() == NodeStatus.FAILED:
                # just remove a failed node
                # print("Failed node %s" % node_id)
                node.set_ended_time(datetime.now())
                self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.FAILED, node)
                self.__running_nodes.remove(node_id)
                # put failed status for the whole workflow

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
            error = "Workflow is not valid.\nSee the logs at %s" % self.__logging_dir
            self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, error)
            # print(error)
            sys.exit(ExitCodes.PLAYBOOK_WRONG_PARAMETER.value)

        if verify_only:
            self.__running_status = WorkflowStatus.ENDED
            return

        if self.__running_status != WorkflowStatus.NOT_STARTED:
            raise Exception("Already running")

        # check the starting node
        self._logger.info("Start from node %s" % start_node)
        if not self.is_node_present(start_node):
            error = "Starting node not exist: %s" % start_node
            self._logger.error("Starting node not exist: %s" % start_node)
            # print(error)
            self.__running_status = WorkflowStatus.FAILED
            self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, error)
            sys.exit(ExitCodes.START_NODE_NOT_EXISTS.value)

        self._set_skipped_nodes(start_node, end_node)
        start_node_object = self.get_node_object(start_node)
        self.__running_status = WorkflowStatus.RUNNING
        self.__running_nodes.append(start_node)
        self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, start_node)

        if isinstance(start_node_object, PNode):
            self.notify_event(WorkflowEventType.NODE_EVENT, NodeStatus.RUNNING, start_node_object)
            start_node_object.run()

        # loop over nodes
        while len(self.__running_nodes):
            self.__run_step(end_node)
            time.sleep(1)

        # print("See the output log to directory %s" % self.__logging_dir)
        if self.get_some_failed_task():
            # print("something failed")
            self.__running_status = WorkflowStatus.FAILED
            self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, '')
            sys.exit(ExitCodes.WORKFLOW_FAILED.value)
        else:
            # print("something not failed")
            self.__running_status = WorkflowStatus.ENDED
            self.notify_event(WorkflowEventType.WORKFLOW_EVENT, self.__running_status, end_node)
