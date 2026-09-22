'''
Tests for the execution loop of AnsibleWorkflow.

The engine advances by polling, so these tests start ``run()`` in a background
thread and wait for the state they expect. No playbook is executed: the
``fake_runner`` fixture decides the outcome of every node.

Note on the terminal status: a run currently settles on WorkflowStatus.FAILED
even when every playbook succeeded (see test_terminal_status.py). These tests
therefore wait for the run to *settle* and assert on what actually happened -
which nodes ran, in which order, with which status - rather than on the
workflow status enum.
'''
import os

import pytest

from ansible_plan.core.models import (
    NodeStatus,
    WorkflowEventType,
    WorkflowListener,
    WorkflowStatus,
)

from conftest import run_in_thread, wait_for

pytestmark = pytest.mark.usefixtures('no_svg')

SERIAL = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: s1
    import_playbook: playbooks/a.yml
  - id: s2
    import_playbook: playbooks/b.yml
'''

PARALLEL = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: b1
    block:
      - id: p1
        import_playbook: playbooks/a.yml
      - id: p2
        import_playbook: playbooks/b.yml
  - id: after
    import_playbook: playbooks/c.yml
'''

WITH_CHECKPOINT = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: before
    import_playbook: playbooks/a.yml
  - id: gate
    checkpoint: true
    description: proceed?
  - id: after
    import_playbook: playbooks/b.yml
'''

TERMINAL = (WorkflowStatus.ENDED, WorkflowStatus.FAILED)


class RecordingListener(WorkflowListener):
    def __init__(self):
        self.events = []

    def notify_event(self, event):
        self.events.append(event)

    def node_events(self):
        return [(event.get_event()[0], event.get_event()[1].get_id())
                for event in self.events
                if event.get_type() == WorkflowEventType.NODE_EVENT]

    def workflow_events(self):
        return [event.get_event() for event in self.events
                if event.get_type() == WorkflowEventType.WORKFLOW_EVENT]


def settle(workflow, message='the run never settled'):
    '''Wait until the loop stops advancing, whatever status it settles on.'''
    wait_for(lambda: workflow.get_running_status() in TERMINAL and not workflow.is_running(),
             message=message)


def statuses(workflow, *node_ids):
    return [workflow.get_node_object(node_id).get_status() for node_id in node_ids]


# --------------------------------------------------------------------------
# happy paths
# --------------------------------------------------------------------------

def test_a_serial_workflow_runs_its_nodes_in_order(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow)
    settle(workflow)

    assert fake_runner.idents() == ['s1', 's2']
    assert statuses(workflow, 's1', 's2') == [NodeStatus.ENDED, NodeStatus.ENDED]


def test_a_parallel_block_starts_its_nodes_together(write_wf, load, fake_runner):
    fake_runner.hold = True
    workflow = load(write_wf(PARALLEL))
    run_in_thread(workflow)

    wait_for(lambda: set(fake_runner.idents()) == {'p1', 'p2'},
             message='both parallel nodes should have started')
    assert 'after' not in fake_runner.idents()


def test_the_node_after_a_block_waits_for_every_branch(write_wf, load, fake_runner):
    fake_runner.hold = True
    workflow = load(write_wf(PARALLEL))
    run_in_thread(workflow)
    wait_for(lambda: set(fake_runner.idents()) == {'p1', 'p2'})

    fake_runner.finish('p1')
    wait_for(lambda: workflow.get_node_object('p1').get_status() == NodeStatus.ENDED)
    assert 'after' not in fake_runner.idents()

    fake_runner.finish('p2')
    wait_for(lambda: 'after' in fake_runner.idents(),
             message='the node after the block never started')


def test_timings_are_recorded_for_each_node(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow)
    settle(workflow)

    telemetry = workflow.get_node_object('s1').get_telemetry()
    assert telemetry['started'] != ''
    assert telemetry['ended'] != ''


def test_the_engine_writes_its_own_log_file(write_wf, load, fake_runner, log_dir):
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow)
    settle(workflow)

    assert os.path.exists(os.path.join(log_dir, 'workflow.log'))


# --------------------------------------------------------------------------
# validation gate
# --------------------------------------------------------------------------

def test_verify_only_validates_without_running(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL))
    workflow.run(verify_only=True)

    assert workflow.get_running_status() == WorkflowStatus.ENDED
    assert fake_runner.calls == []


def test_an_invalid_workflow_fails_before_anything_runs(write_wf, load, fake_runner):
    broken = SERIAL.replace('playbooks/b.yml', 'playbooks/missing.yml')
    workflow = load(write_wf(broken))
    listener = RecordingListener()
    workflow.add_event_listener(listener)

    workflow.run()

    assert workflow.get_running_status() == WorkflowStatus.FAILED
    assert fake_runner.calls == []
    status, errors = listener.workflow_events()[0]
    assert status == WorkflowStatus.FAILED
    assert any('missing.yml' in error for error in errors)


def test_an_unknown_start_node_fails_the_run(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL))
    workflow.run(start_node='nope')

    assert workflow.get_running_status() == WorkflowStatus.FAILED
    assert fake_runner.calls == []


# --------------------------------------------------------------------------
# failure, retry and skip
# --------------------------------------------------------------------------

def test_a_failed_node_stops_the_workflow_advancing(write_wf, load, fake_runner):
    fake_runner.statuses = {'s1': ['failed']}
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow)
    settle(workflow)

    assert fake_runner.idents() == ['s1']
    assert statuses(workflow, 's1', 's2') == [NodeStatus.FAILED, NodeStatus.NOT_STARTED]


def test_restarting_a_failed_node_resumes_the_workflow(write_wf, load, fake_runner):
    fake_runner.statuses = {'s1': ['failed', 'successful']}
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow)
    settle(workflow)

    workflow.restart_failed_node('s1')

    wait_for(lambda: fake_runner.idents() == ['s1', 's1', 's2'],
             message='the workflow did not carry on after the retry')
    wait_for(lambda: statuses(workflow, 's1', 's2') == [NodeStatus.ENDED, NodeStatus.ENDED])


def test_skipping_a_failed_node_resumes_the_workflow(write_wf, load, fake_runner):
    fake_runner.statuses = {'s1': ['failed']}
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow)
    settle(workflow)

    workflow.skip_failed_node('s1')

    wait_for(lambda: fake_runner.idents() == ['s1', 's2'],
             message='the workflow did not continue past the skipped node')
    assert workflow.get_node_object('s1').is_skipped() is True


def test_only_failed_nodes_can_be_restarted_or_skipped(write_wf, load, fake_runner):
    fake_runner.hold = True
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow)
    wait_for(lambda: fake_runner.idents() == ['s1'])

    workflow.restart_failed_node('s1')
    workflow.skip_failed_node('s1')

    assert fake_runner.idents() == ['s1']
    assert workflow.get_node_object('s1').is_skipped() is False


# --------------------------------------------------------------------------
# checkpoints and doubtful mode
# --------------------------------------------------------------------------

def test_a_checkpoint_pauses_the_workflow(write_wf, load, fake_runner):
    workflow = load(write_wf(WITH_CHECKPOINT))
    run_in_thread(workflow)

    wait_for(lambda: workflow.get_running_status() == WorkflowStatus.PAUSED,
             message='the checkpoint should have paused the workflow')
    assert workflow.get_node_object('gate').get_status() == NodeStatus.AWAITING_CONFIRMATION
    assert fake_runner.idents() == ['before']


def test_approving_a_checkpoint_lets_the_workflow_continue(write_wf, load, fake_runner):
    workflow = load(write_wf(WITH_CHECKPOINT))
    run_in_thread(workflow)
    wait_for(lambda: workflow.get_node_object('gate').get_status() == NodeStatus.AWAITING_CONFIRMATION)

    workflow.approve_node('gate')

    wait_for(lambda: fake_runner.idents() == ['before', 'after'],
             message='workflow did not continue after the checkpoint was approved')


def test_disapproving_a_checkpoint_fails_that_node(write_wf, load, fake_runner):
    workflow = load(write_wf(WITH_CHECKPOINT))
    run_in_thread(workflow)
    wait_for(lambda: workflow.get_node_object('gate').get_status() == NodeStatus.AWAITING_CONFIRMATION)

    workflow.disapprove_node('gate')

    settle(workflow)
    assert workflow.get_node_object('gate').get_status() == NodeStatus.FAILED
    assert 'after' not in fake_runner.idents()


def test_doubtful_mode_asks_before_every_playbook(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL), doubtful_mode=True)
    run_in_thread(workflow)

    wait_for(lambda: workflow.get_node_object('s1').get_status() == NodeStatus.AWAITING_CONFIRMATION,
             message='doubtful mode should ask before the first node')
    assert fake_runner.calls == []

    workflow.approve_node('s1')
    wait_for(lambda: workflow.get_node_object('s2').get_status() == NodeStatus.AWAITING_CONFIRMATION,
             message='doubtful mode should ask before every node')
    assert fake_runner.idents() == ['s1']

    workflow.approve_node('s2')
    wait_for(lambda: fake_runner.idents() == ['s1', 's2'])


def test_a_refused_node_in_doubtful_mode_is_skipped(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL), doubtful_mode=True)
    run_in_thread(workflow)
    wait_for(lambda: workflow.get_node_object('s1').get_status() == NodeStatus.AWAITING_CONFIRMATION)

    workflow.disapprove_node('s1')

    wait_for(lambda: workflow.get_node_object('s2').get_status() == NodeStatus.AWAITING_CONFIRMATION,
             message='the workflow should carry on past the refused node')
    assert workflow.get_node_object('s1').get_status() == NodeStatus.SKIPPED
    assert fake_runner.calls == []


def test_approving_a_node_that_was_not_asked_about_does_nothing(write_wf, load, fake_runner):
    fake_runner.hold = True
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow)
    wait_for(lambda: fake_runner.idents() == ['s1'])

    workflow.approve_node('s2')
    workflow.disapprove_node('s2')

    assert fake_runner.idents() == ['s1']


# --------------------------------------------------------------------------
# pause, resume and stop
# --------------------------------------------------------------------------

def test_pause_holds_the_loop_and_resume_releases_it(write_wf, load, fake_runner):
    fake_runner.hold = True
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow)
    wait_for(lambda: fake_runner.idents() == ['s1'])

    workflow.pause()
    assert workflow.get_running_status() == WorkflowStatus.PAUSED

    # the node completes, but the paused loop must not promote its successor
    fake_runner.finish('s1')
    assert fake_runner.idents() == ['s1']

    workflow.resume()
    assert workflow.get_running_status() == WorkflowStatus.RUNNING
    wait_for(lambda: 's2' in fake_runner.idents(),
             message='resume did not restart the loop')


def test_a_graceful_stop_waits_for_the_running_node(write_wf, load, fake_runner):
    fake_runner.hold = True
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow)
    wait_for(lambda: fake_runner.idents() == ['s1'])

    workflow.stop()
    assert workflow.get_running_status() == WorkflowStatus.STOPPING
    # graceful: the playbook is left alone, it is only not followed by s2
    assert workflow.get_node_object('s1')._cancel_callback() is False

    fake_runner.finish('s1')

    settle(workflow)
    assert workflow.get_running_status() == WorkflowStatus.FAILED
    assert 's2' not in fake_runner.idents()


def test_a_hard_stop_cancels_the_running_node(write_wf, load, fake_runner):
    fake_runner.hold = True
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow)
    wait_for(lambda: fake_runner.idents() == ['s1'])

    workflow.stop('hard')

    # the cancel callback is what ansible-runner polls to abort the playbook
    assert workflow.get_node_object('s1')._cancel_callback() is True

    fake_runner.finish('s1', 'canceled')
    settle(workflow)
    assert workflow.get_node_object('s1').get_status() == NodeStatus.STOPPED


# --------------------------------------------------------------------------
# partial runs
# --------------------------------------------------------------------------

def test_starting_from_a_later_node_skips_what_precedes_it(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow, start_node='s2')
    settle(workflow)

    assert fake_runner.idents() == ['s2']
    assert workflow.get_node_object('s1').is_skipped() is True


def test_ending_early_stops_before_the_end_node(write_wf, load, fake_runner):
    # --end-to-node is exclusive: the named node is the boundary and is not
    # executed, which mirrors the default end node _e being a no-op marker.
    # Only what comes *after* it is marked skipped.
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow, end_node='s2')
    settle(workflow)

    assert fake_runner.idents() == ['s1']
    assert workflow.get_node_object('s2').get_status() == NodeStatus.NOT_STARTED
    assert workflow.get_node_object('s2').is_skipped() is False
    assert workflow.get_node_object('_e').is_skipped() is True


def test_skipped_nodes_are_not_executed(write_wf, load, fake_runner):
    workflow = load(write_wf(PARALLEL))
    workflow.set_skipped_nodes(['p1'])
    run_in_thread(workflow)
    settle(workflow)

    assert set(fake_runner.idents()) == {'p2', 'after'}


def test_filtered_nodes_run_alone(write_wf, load, fake_runner):
    workflow = load(write_wf(PARALLEL))
    workflow.set_filtered_nodes(['_s', '_e', 'b1', 'p1', 'after'])
    run_in_thread(workflow)
    settle(workflow)

    assert set(fake_runner.idents()) == {'p1', 'after'}


# --------------------------------------------------------------------------
# events
# --------------------------------------------------------------------------

def test_listeners_are_notified_of_node_and_workflow_events(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL))
    listener = RecordingListener()
    workflow.add_event_listener(listener)
    run_in_thread(workflow)
    settle(workflow)

    node_events = listener.node_events()
    assert (NodeStatus.RUNNING, 's1') in node_events
    assert (NodeStatus.ENDED, 's1') in node_events
    assert (NodeStatus.RUNNING, 's2') in node_events
    assert listener.workflow_events()[0][0] == WorkflowStatus.RUNNING


def test_running_a_settled_workflow_again_is_refused(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow)
    settle(workflow)

    with pytest.raises(Exception, match='Already running'):
        workflow.run()
