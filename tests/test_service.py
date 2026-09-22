'''
Tests for the FastAPI backend.

The backend keeps a single workflow in a module global, so ``client`` resets
it between tests. TestClient runs background tasks synchronously once the
handler returns, which means POST /workflow only comes back after the run has
settled - every workflow used here is therefore small or verify-only.
'''
import os
import threading

import pytest
from fastapi.testclient import TestClient

from ansible_plan import service
from ansible_plan.core.models import WorkflowStatus

from conftest import wait_for

pytestmark = pytest.mark.usefixtures('no_svg')

SERIAL = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: s1
    import_playbook: playbooks/a.yml
  - id: block1
    block:
      - id: s2
        import_playbook: playbooks/b.yml
'''

BROKEN = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: s1
    import_playbook: playbooks/a.yml
    not_a_real_key: true
'''


@pytest.fixture
def client():
    service.current_workflow = None
    with TestClient(service.app) as test_client:
        yield test_client
    service.current_workflow = None


@pytest.fixture
def start(client, write_wf, log_dir):
    '''POST a workflow to the backend and return the response.'''
    def _start(contents=SERIAL, name='workflow.yml', **overrides):
        payload = {
            'workflow_file': write_wf(contents, name=name),
            'log_dir': log_dir,
            'log_level': 'warning',
        }
        payload.update(overrides)
        return client.post('/workflow', json=payload)
    return _start


@pytest.fixture
def start_running(client, write_wf, log_dir):
    '''
    Start a workflow that actually executes its nodes.

    ``AnsibleWorkflow.run`` does not return on its own once a run settles (it
    parks waiting for a retry decision), and TestClient waits for background
    tasks, so the request is issued from a thread and released with stop()
    the way the CLI does it through /shutdown.
    '''
    responses = {}

    def _start(contents=SERIAL, name='workflow.yml', **overrides):
        payload = {
            'workflow_file': write_wf(contents, name=name),
            'log_dir': log_dir,
            'log_level': 'warning',
            'verify_only': False,
        }
        payload.update(overrides)

        def post():
            responses['value'] = client.post('/workflow', json=payload)

        thread = threading.Thread(target=post, daemon=True)
        thread.start()

        wait_for(lambda: service.current_workflow is not None
                 and service.current_workflow.get_running_status() in (
                     WorkflowStatus.ENDED, WorkflowStatus.FAILED, WorkflowStatus.PAUSED),
                 message='the backend never settled')
        service.current_workflow.stop()
        thread.join(timeout=10)
        return responses.get('value')

    return _start


# --------------------------------------------------------------------------
# lifecycle
# --------------------------------------------------------------------------

def test_health_is_always_available(client):
    response = client.get('/health')

    assert response.status_code == 200
    assert response.json() == {'status': 'ok'}


def test_status_before_anything_is_started(client):
    assert client.get('/workflow').json() == {'status': WorkflowStatus.NOT_STARTED.value}


def test_starting_a_workflow_runs_it(start_running, fake_runner):
    response = start_running()

    assert response.status_code == 200
    assert response.json() == {'status': WorkflowStatus.RUNNING.value}
    assert fake_runner.idents() == ['s1', 's2']


def test_verify_only_does_not_run_the_playbooks(start, client, fake_runner):
    start(verify_only=True)

    assert fake_runner.calls == []
    assert client.get('/workflow').json()['status'] == WorkflowStatus.ENDED.value


def test_posting_the_same_file_again_reconnects(start, fake_runner):
    start(verify_only=True)
    response = start(verify_only=True)

    assert response.status_code == 200
    assert response.json() == {'status': 'reconnected'}


def test_posting_a_different_file_is_a_conflict(start, fake_runner, workdir):
    start(verify_only=True)

    response = start(name='other.yml', verify_only=True)

    assert response.status_code == 409
    detail = response.json()['detail']
    assert detail['running_workflow_file'] == str(workdir / 'workflow.yml')
    assert 'already running' in detail['message']


def test_an_invalid_workflow_is_reported_as_unprocessable(start, client, fake_runner):
    response = start(BROKEN, verify_only=True)

    assert response.status_code == 422
    assert response.json()['detail']['validation_errors']

    # the failed workflow is kept so the UI can show why it did not start
    status = client.get('/workflow').json()
    assert status['status'] == WorkflowStatus.FAILED.value
    assert status['validation_errors']


def test_a_workflow_that_cannot_be_read_is_reported(start, fake_runner, workdir):
    response = start(workflow_file=str(workdir / 'nope.yml'), verify_only=True)

    assert response.status_code == 422


# --------------------------------------------------------------------------
# reading the workflow
# --------------------------------------------------------------------------

def test_nodes_are_listed_with_their_status_and_type(start, client, fake_runner):
    start(verify_only=True)
    nodes = {node['id']: node for node in client.get('/workflow/nodes').json()}

    assert nodes['s1']['type'] == 'playbook'
    assert nodes['s1']['status'] == 'not_started'
    assert nodes['s1']['playbook'].endswith('playbooks/a.yml')
    assert nodes['s1']['inventory'].endswith('inventory.ini')
    assert 'started' in nodes['s1'] and 'ended' in nodes['s1']
    assert nodes['block1']['type'] == 'block'
    # blocks carry the strategy of their children for the UI to label them
    assert nodes['block1']['strategy'] == 'parallel'


def test_nodes_are_empty_before_a_workflow_is_started(client):
    assert client.get('/workflow/nodes').json() == []


def test_the_graph_endpoint_returns_the_hierarchy(start, client, fake_runner):
    start(verify_only=True)
    edges = client.get('/workflow/graph').json()['edges']

    assert ['_root', 's1'] in edges
    assert ['block1', 's2'] in edges


def test_the_graph_endpoint_needs_a_workflow(client):
    assert client.get('/workflow/graph').status_code == 404


def test_node_stdout_is_empty_until_the_artifact_exists(start, client, fake_runner):
    start(verify_only=True)

    assert client.get('/workflow/node/s1/stdout').json() == {'stdout': ''}


def test_node_stdout_is_read_from_the_artifact_directory(start, client, fake_runner, log_dir):
    start(verify_only=True)
    os.makedirs(os.path.join(log_dir, 's1'))
    with open(os.path.join(log_dir, 's1', 'stdout'), 'w') as handle:
        handle.write('PLAY RECAP')

    assert client.get('/workflow/node/s1/stdout').json() == {'stdout': 'PLAY RECAP'}


def test_only_playbook_nodes_have_stdout(start, client, fake_runner):
    start(verify_only=True)

    assert client.get('/workflow/node/block1/stdout').status_code == 404


# --------------------------------------------------------------------------
# controlling the workflow
# --------------------------------------------------------------------------

@pytest.mark.parametrize('path, payload', [
    ('/workflow/stop', {'mode': 'graceful'}),
    ('/workflow/pause', None),
    ('/workflow/resume', None),
])
def test_control_endpoints_need_a_live_workflow(client, path, payload):
    response = client.post(path, json=payload) if payload else client.post(path)

    assert response.status_code == 404


@pytest.mark.parametrize('path', [
    '/workflow/node/s1/restart',
    '/workflow/node/s1/skip',
    '/workflow/node/s1/approve',
    '/workflow/node/s1/disapprove',
])
def test_node_endpoints_need_a_workflow(client, path):
    assert client.post(path).status_code == 404


@pytest.mark.parametrize('action', ['restart', 'skip', 'approve', 'disapprove'])
def test_node_actions_are_ignored_when_the_node_is_not_in_that_state(start, client,
                                                                    fake_runner, action):
    start(verify_only=True)

    response = client.post('/workflow/node/s1/%s' % action)

    assert response.status_code == 200
    assert fake_runner.calls == []


def test_shutdown_is_refused_while_a_workflow_runs(start, client, fake_runner, monkeypatch):
    start(verify_only=True)
    service.current_workflow.set_status(WorkflowStatus.RUNNING)

    assert client.post('/shutdown').status_code == 409


def test_shutdown_signals_the_process_when_idle(start, client, fake_runner, monkeypatch):
    signals = []
    monkeypatch.setattr(service.os, 'kill', lambda pid, sig: signals.append((pid, sig)))
    start(verify_only=True)

    response = client.post('/shutdown')

    assert response.status_code == 200
    assert signals == [(os.getpid(), service.signal.SIGTERM)]


# --------------------------------------------------------------------------
# request parsing
# --------------------------------------------------------------------------

def test_run_options_are_passed_through_to_the_engine(start_running, fake_runner):
    start_running(skip_nodes=['s1'], check_mode=True, verbosity=3)

    assert fake_runner.idents() == ['s2']
    call = fake_runner.call_for('s2')
    assert '--check' in call['cmdline']
    assert call['verbosity'] == 3


def test_extra_vars_and_templating_reach_the_playbooks(start_running, fake_runner):
    workflow = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: s1
    import_playbook: playbooks/a.yml
    limit: "{{ target }}"
'''
    start_running(workflow,
                  extra_vars={'from_cli': 'yes'},
                  input_templating={'target': 'first_hostname'})

    call = fake_runner.call_for('s1')
    assert call['extravars'] == {'from_cli': 'yes'}
    assert call['limit'] == 'first_hostname'


def test_a_workflow_file_is_the_only_required_field(client, write_wf, log_dir):
    response = client.post('/workflow', json={})

    assert response.status_code == 422
