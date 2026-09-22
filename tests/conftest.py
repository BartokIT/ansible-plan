'''
Shared fixtures for the test suite.

Two things make this project awkward to test and are handled here once:

* ``PNode.run`` shells out to ansible-runner. The ``fake_runner`` fixture
  replaces ``ansible_runner.run_async`` so no playbook is ever executed and
  tests can decide the outcome of each node.
* every component builds its logger with ``logging.getLogger(<fixed name>)``
  and attaches a file handler to it. Those loggers are process global, so
  without ``reset_loggers`` handlers pile up across tests and keep writing
  into deleted temporary directories.
'''
import logging
import os
import shutil
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ansible_plan.core import models  # noqa: E402
from ansible_plan.core.engine import AnsibleWorkflow  # noqa: E402
from ansible_plan.core.loader import WorkflowYamlLoader  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures')

# every logger name the code base creates a file handler for
LOGGER_NAMES = ['main', 'AnsibleWorkflow', 'WorkflowYamlLoader',
                'StdoutWorkflowOutput', 'TextualWorkflowOutput']


@pytest.fixture(autouse=True)
def reset_loggers():
    '''Detach the file handlers the components install on global loggers.'''
    yield
    for name in LOGGER_NAMES:
        logger = logging.getLogger(name)
        for handler in list(logger.handlers):
            handler.close()
            logger.removeHandler(handler)


@pytest.fixture(autouse=True)
def stop_leftover_runs():
    '''
    Shut down every run a test started.

    A test that deliberately leaves a run in flight - anything using
    ``fake_runner.hold`` - would otherwise keep driving it afterwards: the
    engine thread is a daemon and survives the test, and when it starts the
    next node it calls whatever ``ansible_runner.run_async`` points at by
    then, which is the *next* test's double. The stray call lands in that
    test's recorded calls and breaks assertions that have nothing to do with
    it.
    '''
    yield
    while _ACTIVE_RUNS:
        workflow, thread = _ACTIVE_RUNS.pop()
        workflow.stop('hard')
        # release whatever the run is waiting on, or it never leaves its loop
        for runner in _FAKE_RUNNERS:
            runner.finish_all('canceled')
        thread.join(timeout=10)
    del _FAKE_RUNNERS[:]


@pytest.fixture
def log_dir(tmp_path):
    '''The directory a run writes its logs and ansible-runner artifacts to.'''
    path = tmp_path / 'logs'
    path.mkdir()
    return str(path)


@pytest.fixture
def workdir(tmp_path):
    '''
    An isolated copy of the fixture skeleton (inventories, playbooks, vault
    script). Workflow files are written into it by the ``write_wf`` helper, so
    relative paths inside them resolve the way they do for a real user.
    '''
    path = tmp_path / 'wf'
    shutil.copytree(FIXTURES, str(path))
    return path


@pytest.fixture
def write_wf(workdir):
    '''Write a workflow file into the work directory and return its path.'''
    def _write(contents, name='workflow.yml'):
        target = workdir / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(contents)
        return str(target)
    return _write


@pytest.fixture
def load(log_dir):
    '''Parse a workflow file and return the resulting AnsibleWorkflow.'''
    def _load(workflow_file, extra_vars=None, **kwargs):
        # 'warning' keeps the very chatty debug/info records out of the
        # pytest log capture; the log files are written regardless
        loader = WorkflowYamlLoader(workflow_file, log_dir, 'warning', **kwargs)
        return loader.parse(extra_vars or {})
    return _load


class _FakeThread:
    '''Stands in for the thread ansible-runner hands back.'''

    def __init__(self, job):
        self._job = job

    def is_alive(self):
        return not self._job.done


class _FakeRunner:
    '''Stands in for the runner object: the engine only reads ``status``.'''

    def __init__(self, job):
        self._job = job

    @property
    def status(self):
        return self._job.status


class FakeJob:
    def __init__(self, ident, status, done):
        self.ident = ident
        self.status = status
        self.done = done

    def finish(self, status='successful'):
        self.status = status
        self.done = True


_ACTIVE_RUNS = []
_FAKE_RUNNERS = []


class FakeAnsibleRunner:
    '''
    Records every ``run_async`` call and decides what each node does.

    ``statuses`` maps a node id to the outcomes of its successive runs, which
    is what retry tests need; anything not listed gets ``default_status``.
    When ``hold`` is set the node stays alive until ``finish(ident)`` is
    called, which is how a workflow is kept mid-flight.
    '''

    def __init__(self):
        self.calls = []
        self.jobs = {}
        self.statuses = {}
        self.default_status = 'successful'
        self.hold = False
        _FAKE_RUNNERS.append(self)

    def run_async(self, **kwargs):
        self.calls.append(kwargs)
        ident = kwargs.get('ident')
        queued = self.statuses.get(ident)
        status = queued.pop(0) if queued else self.default_status
        job = FakeJob(ident, status, done=not self.hold)
        self.jobs[ident] = job
        return _FakeThread(job), _FakeRunner(job)

    # helpers for the tests -------------------------------------------------
    def finish(self, ident, status='successful'):
        self.jobs[ident].finish(status)

    def finish_all(self, status='successful'):
        for job in self.jobs.values():
            job.finish(status)

    def idents(self):
        return [call['ident'] for call in self.calls]

    def call_for(self, ident):
        return next(call for call in self.calls if call['ident'] == ident)


@pytest.fixture
def fake_runner(monkeypatch):
    fake = FakeAnsibleRunner()
    monkeypatch.setattr(models.ansible_runner, 'run_async', fake.run_async)
    return fake


@pytest.fixture
def no_svg(monkeypatch):
    '''
    Skip the graphviz rendering ``AnsibleWorkflow.run`` starts with: it is
    slow and writes files none of these tests look at.
    '''
    monkeypatch.setattr('ansible_plan.core.drawer.generate_workflow_svg',
                        lambda workflow, output_path_prefix: None)


def wait_for(predicate, timeout=15, message='condition not reached'):
    '''Poll ``predicate`` until it holds; the engine advances by polling too.'''
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    raise AssertionError(message)


def run_in_thread(workflow: AnsibleWorkflow, **kwargs):
    '''
    Start ``AnsibleWorkflow.run`` in the background.

    The run is registered so ``stop_leftover_runs`` can shut it down at the
    end of the test.
    '''
    thread = threading.Thread(target=workflow.run, kwargs=kwargs, daemon=True)
    thread.start()
    _ACTIVE_RUNS.append((workflow, thread))
    return thread
