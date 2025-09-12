from enum import Enum
import typing
from datetime import datetime
import abc
import os
import logging
import ansible_runner
from .exceptions import AnsibleWorkflowPlaybookNodeCheck


class WorkflowStatus(Enum):
    """ Define the character for the application"""
    NOT_STARTED = 'not_started'
    RUNNING = 'running'
    PAUSED = 'paused'
    STOPPING = 'stopping'
    ENDED = 'ended'
    FAILED = 'failed'


class NodeStatus(Enum):
    """ Define the character for the application"""
    RUNNING = 'running'
    PRE_RUNNING = 'pre_running'
    AWAITING_CONFIRMATION = 'awaiting_confirmation'
    ENDED = 'ended'
    FAILED = 'failed'
    SKIPPED = 'skipped'
    NOT_STARTED = 'not_started'
    STOPPED = 'stopped'


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
        self._status: typing.Optional[NodeStatus] = None

    def get_id(self) -> str:
        return self.__id

    def set_logger(self, logger: logging.Logger):
        self._logger = logger

    def set_skipped(self):
        self.__skipped = True

    def is_skipped(self):
        return self.__skipped

    def set_status(self, status: NodeStatus):
        self._status = status

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
        if self._status:
            return self._status
        return NodeStatus.NOT_STARTED

    def get_type(self):
        return 'block'


class PNode(Node):
    def __init__(self, id, playbook, inventory, artifact_dir, limit=None, project_path=None, extra_vars={}, vault_ids=[], check_mode=False, diff_mode=True, verbosity=1, description='', reference=''):
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
        self.__hard_stop = False

    def check_node_input(self):
        # convert project path in absolute path
        if not os.path.isabs(self.__project_path):
            self.__project_path = os.path.abspath(self.__project_path)

        if not os.path.exists(self.__project_path):
            raise AnsibleWorkflowPlaybookNodeCheck(
                "Node %s project path %s doesn't exists" % (self.get_id(), self.__project_path)
            )

        if self.__inventory is None:
            raise AnsibleWorkflowPlaybookNodeCheck("Node %s inventory not set" % self.get_id())
        elif not os.path.exists(self.__inventory):
            raise AnsibleWorkflowPlaybookNodeCheck(
                "Node %s inventory doesn't exists: %s" % (self.get_id(), self.__inventory)
            )
        else:
            self.__inventory = os.path.abspath(self.__inventory)

        if self.__project_path and not os.path.isabs(self.__playbook):
            self.__playbook = os.path.join(self.__project_path, self.__playbook)

        if not os.path.exists(self.__playbook):
            raise AnsibleWorkflowPlaybookNodeCheck(
                "Node %s playbook doesn't exists: %s" % (self.get_id(), self.__playbook)
            )

    def get_verbosity(self):
        return self.__verbosity

    def set_verbosity(self, verbosity):
        self.__verbosity = verbosity

    def get_status(self):
        if self._status:
            return self._status
        if self.is_skipped():
            return NodeStatus.SKIPPED
        elif self.__thread is None:
            return NodeStatus.NOT_STARTED
        else:
            # print("Node %s status is %s - error is %s" % (self.get_id(), self.__runner.errored, self.__runner.status ))
            if self.__thread.is_alive():
                return NodeStatus.RUNNING
            elif self.is_canceled():
                return NodeStatus.STOPPED
            elif self.is_failed():
                return NodeStatus.FAILED
            else:
                return NodeStatus.ENDED

    def get_type(self):
        return 'playbook'

    def is_canceled(self):
        return self.__runner.status == 'canceled'

    def is_failed(self):
        return self.__runner.status == 'failed'

    def get_playbook(self):
        return self.__playbook

    def get_inventory(self):
        return self.__inventory

    def get_extravars(self):
        return self.__extravars

    def get_description(self):
        return self.__description

    def get_reference(self):
        return self.__reference

    def stop(self):
        self._logger.info("Stopping node %s" % self.get_id())
        self.__hard_stop = True

    def _cancel_callback(self):
        return self.__hard_stop

    def reset_status(self):
        self.__thread = None
        self.__runner = None
        self._started_time = None
        self._ended_time = None

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
                                                                cancel_callback=self._cancel_callback,
                                                                # vault_ids=self.__vault_ids,
                                                                cmdline=playbook_cmd_line,
                                                                extravars=self.__extravars,
                                                                quiet=True)


class INode(Node):
    def __init__(self, id, description='', reference=''):
        super(INode, self).__init__(id)
        self.__description = description
        self.__reference = reference

    def get_status(self):
        if self._status:
            return self._status
        return NodeStatus.NOT_STARTED

    def get_type(self):
        return 'info'

    def get_description(self):
        return self.__description

    def get_reference(self):
        return self.__reference


class CNode(Node):
    def __init__(self, id, description='', reference=''):
        super(CNode, self).__init__(id)
        self.__description = description
        self.__reference = reference

    def get_status(self):
        if self._status:
            return self._status
        return NodeStatus.NOT_STARTED

    def get_type(self):
        return 'checkpoint'

    def get_description(self):
        return self.__description

    def get_reference(self):
        return self.__reference


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
