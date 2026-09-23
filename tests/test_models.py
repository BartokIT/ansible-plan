'''
Tests for the node classes: input checking, the status a node derives from its
runner, and the arguments handed to ansible-runner.
'''
import os
import stat
from datetime import datetime

import pytest

from ansible_plan.core.exceptions import AnsibleWorkflowPlaybookNodeCheck
from ansible_plan.core.models import (
    ARTIFACT_DIR_MODE,
    ARTIFACT_FILE_MODE,
    BNode,
    CNode,
    INode,
    NodeStatus,
    PNode,
    WorkflowEvent,
    WorkflowEventType,
    WorkflowStatus,
)


@pytest.fixture
def pnode(workdir, log_dir):
    '''A playbook node pointing at the fixture skeleton.'''
    def _build(**overrides):
        params = dict(id='n1',
                      playbook='playbooks/a.yml',
                      inventory=str(workdir / 'inventory.ini'),
                      artifact_dir=log_dir,
                      project_path=str(workdir))
        params.update(overrides)
        return PNode(**params)
    return _build


# --------------------------------------------------------------------------
# input checking
# --------------------------------------------------------------------------

def test_check_accepts_a_well_formed_node(pnode, workdir):
    node = pnode()
    node.check_node_input()

    assert node.get_playbook() == str(workdir / 'playbooks' / 'a.yml')
    assert os.path.isabs(node.get_inventory())


def test_check_rejects_a_missing_inventory(pnode):
    with pytest.raises(AnsibleWorkflowPlaybookNodeCheck, match='inventory not set'):
        pnode(inventory=None).check_node_input()


def test_check_rejects_a_nonexistent_inventory(pnode, workdir):
    with pytest.raises(AnsibleWorkflowPlaybookNodeCheck, match="inventory doesn't exists"):
        pnode(inventory=str(workdir / 'nope.ini')).check_node_input()


def test_check_rejects_a_nonexistent_playbook(pnode):
    with pytest.raises(AnsibleWorkflowPlaybookNodeCheck, match="playbook doesn't exists"):
        pnode(playbook='playbooks/nope.yml').check_node_input()


def test_check_rejects_a_nonexistent_project_path(pnode, workdir):
    with pytest.raises(AnsibleWorkflowPlaybookNodeCheck, match="project path"):
        pnode(project_path=str(workdir / 'nope')).check_node_input()


def test_check_makes_a_relative_project_path_absolute(pnode, workdir, monkeypatch):
    monkeypatch.chdir(workdir)
    node = pnode(project_path='.')
    node.check_node_input()

    assert node.get_playbook() == os.path.join(str(workdir), 'playbooks/a.yml')


# --------------------------------------------------------------------------
# status
# --------------------------------------------------------------------------

def test_a_playbook_node_starts_out_not_started(pnode):
    assert pnode().get_status() == NodeStatus.NOT_STARTED


def test_status_follows_the_runner(pnode, fake_runner):
    fake_runner.hold = True
    node = pnode()
    node.run()

    assert node.get_status() == NodeStatus.RUNNING

    fake_runner.finish('n1', 'successful')
    assert node.get_status() == NodeStatus.ENDED


def test_a_failed_runner_makes_a_failed_node(pnode, fake_runner):
    node = pnode()
    fake_runner.default_status = 'failed'
    node.run()

    assert node.get_status() == NodeStatus.FAILED


def test_a_canceled_runner_makes_a_stopped_node(pnode, fake_runner):
    node = pnode()
    fake_runner.default_status = 'canceled'
    node.run()

    assert node.get_status() == NodeStatus.STOPPED


def test_a_skipped_node_reports_skipped(pnode):
    node = pnode()
    node.set_skipped()

    assert node.is_skipped() is True
    assert node.get_status() == NodeStatus.SKIPPED


def test_an_explicit_status_pins_the_node(pnode, fake_runner):
    node = pnode()
    fake_runner.default_status = 'failed'
    node.run()
    node.set_status(NodeStatus.AWAITING_CONFIRMATION)

    assert node.get_status() == NodeStatus.AWAITING_CONFIRMATION


def test_reset_status_puts_the_node_back_to_not_started(pnode, fake_runner):
    node = pnode()
    node.run()
    node.reset_status()

    assert node.get_status() == NodeStatus.NOT_STARTED
    assert node.get_telemetry() == {'started': '', 'ended': ''}


@pytest.mark.parametrize('node_class, expected_type', [
    (BNode, 'block'),
    (CNode, 'checkpoint'),
    (INode, 'info'),
])
def test_non_playbook_nodes_report_their_type(node_class, expected_type):
    node = node_class('x')

    assert node.get_type() == expected_type
    assert node.get_status() == NodeStatus.NOT_STARTED


def test_a_playbook_node_reports_its_type(pnode):
    assert pnode().get_type() == 'playbook'


# --------------------------------------------------------------------------
# identity and telemetry
# --------------------------------------------------------------------------

def test_nodes_are_identified_by_id_alone():
    assert BNode('same') == INode('same')
    assert len({BNode('same'), INode('same')}) == 1
    assert str(PNode('n1', 'p.yml', 'i.ini', '/tmp')) == 'playbook[n1]'


def test_telemetry_is_formatted_as_wall_clock_times(pnode):
    node = pnode()
    node.set_started_time(datetime(2026, 1, 2, 3, 4, 5))
    node.set_ended_time(datetime(2026, 1, 2, 3, 4, 9))

    assert node.get_telemetry() == {'started': '03:04:05', 'ended': '03:04:09'}


# --------------------------------------------------------------------------
# the ansible-runner call
# --------------------------------------------------------------------------

def test_run_passes_the_expected_arguments(pnode, fake_runner, workdir, log_dir):
    node = pnode(limit='first_hostname', extra_vars={'k': 'v'}, verbosity=3)
    # run() only makes paths absolute, it does not resolve them against the
    # project path; the engine always validates before running, so do the same
    node.check_node_input()
    node.run()
    call = fake_runner.call_for('n1')

    assert call['playbook'] == str(workdir / 'playbooks' / 'a.yml')
    assert call['inventory'] == str(workdir / 'inventory.ini')
    assert call['limit'] == 'first_hostname'
    assert call['extravars'] == {'k': 'v'}
    assert call['verbosity'] == 3
    assert call['artifact_dir'] == log_dir
    assert call['quiet'] is True


def test_diff_mode_is_on_by_default_and_check_mode_is_opt_in(pnode, fake_runner):
    pnode().run()
    assert fake_runner.call_for('n1')['cmdline'].strip() == '--diff'

    pnode(id='n2', check_mode=True).run()
    assert fake_runner.call_for('n2')['cmdline'].split() == ['--check', '--diff']


def test_vault_ids_become_vault_id_flags(pnode, fake_runner):
    pnode(vault_ids=['ID1@/tmp/script.py', 'ID2@/tmp/script.py']).run()

    assert fake_runner.call_for('n1')['cmdline'].split()[:4] == [
        '--vault-id', 'ID1@/tmp/script.py', '--vault-id', 'ID2@/tmp/script.py',
    ]


def test_a_project_path_sets_the_collections_path(pnode, fake_runner, workdir):
    pnode().run()

    assert fake_runner.call_for('n1')['envvars'] == {
        'ANSIBLE_COLLECTIONS_PATHS': str(workdir / 'collections')
    }


def test_rerunning_a_node_uses_a_fresh_artifact_ident(pnode, fake_runner, log_dir):
    node = pnode()
    node.run()
    assert node.ident == 'n1'

    # ansible-runner would have created this; the fake never does
    os.makedirs(os.path.join(log_dir, 'n1'))
    node.run()
    assert node.ident == 'n1_1'

    os.makedirs(os.path.join(log_dir, 'n1_1'))
    node.run()
    assert node.ident == 'n1_2'


def test_stop_flips_the_cancel_callback(pnode, fake_runner):
    node = pnode()
    node.run()
    assert node._cancel_callback() is False

    node.stop()
    assert node._cancel_callback() is True


def test_artifacts_handler_relaxes_the_permissions_runner_forces(pnode, tmp_path):
    artifacts = tmp_path / 'artifacts'
    (artifacts / 'inner').mkdir(parents=True)
    artifact_file = artifacts / 'inner' / 'stdout'
    artifact_file.write_text('output')
    os.chmod(str(artifacts / 'inner'), 0o700)
    os.chmod(str(artifact_file), 0o600)

    pnode()._relax_artifact_permissions(str(artifacts))

    assert stat.S_IMODE(os.stat(str(artifacts / 'inner')).st_mode) == ARTIFACT_DIR_MODE
    assert stat.S_IMODE(os.stat(str(artifact_file)).st_mode) == ARTIFACT_FILE_MODE


def test_artifacts_handler_survives_unwritable_entries(pnode, tmp_path, monkeypatch):
    artifacts = tmp_path / 'artifacts'
    artifacts.mkdir()
    (artifacts / 'stdout').write_text('output')

    def refuse(*args, **kwargs):
        raise OSError('operation not permitted')

    monkeypatch.setattr(os, 'chmod', refuse)
    pnode()._relax_artifact_permissions(str(artifacts))  # must not raise


# --------------------------------------------------------------------------
# events
# --------------------------------------------------------------------------

def test_workflow_event_carries_type_event_and_content():
    event = WorkflowEvent(WorkflowEventType.NODE_EVENT, NodeStatus.ENDED, 'payload')

    assert event.get_type() == WorkflowEventType.NODE_EVENT
    assert event.get_event() == (NodeStatus.ENDED, 'payload')
    assert 'NODE_EVENT' in str(event)


def test_status_enums_expose_the_wire_values():
    assert WorkflowStatus.RUNNING.value == 'running'
    assert NodeStatus.AWAITING_CONFIRMATION.value == 'awaiting_confirmation'
