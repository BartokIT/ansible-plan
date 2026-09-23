'''
The terminal status of a run.

A run settles on FAILED only when something actually failed. Nodes that simply
never ran do not make a workflow failed, which matters because two internal
nodes are never executed:

* ``_root``, the hierarchy root the UIs draw their tree from, is in the
  execution graph but is never part of a run;
* ``_e``, the end marker, is deliberately not promoted by ``__run_step``
  (``if next_node_id != end_node``).

Both stay NOT_STARTED. Counting them as failures used to make every successful
workflow settle on FAILED, parked on "waiting for retry" with nothing that
could be retried - and ``run()`` never returned, since only ``stop()`` broke
that loop.
'''
import pytest

from ansible_plan.core.models import NodeStatus, WorkflowStatus

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


def run_to_completion(workflow, **kwargs):
    thread = run_in_thread(workflow, **kwargs)
    wait_for(lambda: workflow.get_running_status() in (WorkflowStatus.ENDED,
                                                       WorkflowStatus.FAILED)
             and not workflow.is_running())
    return thread


# --------------------------------------------------------------------------
# a run that worked
# --------------------------------------------------------------------------

def test_a_fully_successful_workflow_reports_ended(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL))
    run_to_completion(workflow)

    assert workflow.get_running_status() == WorkflowStatus.ENDED


def test_a_successful_run_returns_on_its_own(write_wf, load, fake_runner):
    # the loop used to park on the failure branch for ever, so the run only
    # ended when something called stop()
    workflow = load(write_wf(SERIAL))
    thread = run_to_completion(workflow)

    thread.join(timeout=5)
    assert thread.is_alive() is False


def test_the_boundary_nodes_are_still_never_executed(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL))
    run_to_completion(workflow)

    assert workflow.get_node_object('s1').get_status() == NodeStatus.ENDED
    assert workflow.get_node_object('s2').get_status() == NodeStatus.ENDED
    # ... these two never move, and that is not a failure
    assert workflow.get_node_object('_root').get_status() == NodeStatus.NOT_STARTED
    assert workflow.get_node_object('_e').get_status() == NodeStatus.NOT_STARTED
    assert workflow.get_some_failed_task() is False


def test_a_run_stopped_early_by_the_end_node_reports_ended(write_wf, load, fake_runner):
    # --end-to-node is exclusive, so the named node never runs; not a failure
    workflow = load(write_wf(SERIAL))
    run_to_completion(workflow, end_node='s2')

    assert workflow.get_node_object('s2').get_status() == NodeStatus.NOT_STARTED
    assert workflow.get_running_status() == WorkflowStatus.ENDED


def test_verify_only_reports_ended(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL))
    workflow.run(verify_only=True)

    assert workflow.get_running_status() == WorkflowStatus.ENDED


# --------------------------------------------------------------------------
# a run that did not work must still say so
# --------------------------------------------------------------------------

def test_a_failed_node_still_makes_the_workflow_failed(write_wf, load, fake_runner):
    fake_runner.statuses = {'s1': ['failed']}
    workflow = load(write_wf(SERIAL))
    run_to_completion(workflow)

    assert workflow.get_running_status() == WorkflowStatus.FAILED
    assert workflow.get_some_failed_task() is True


def test_the_failed_nodes_are_the_ones_a_retry_can_act_on(write_wf, load, fake_runner):
    fake_runner.statuses = {'s1': ['failed']}
    workflow = load(write_wf(SERIAL))
    run_to_completion(workflow)

    assert workflow.get_failed_nodes() == ['s1']

    workflow.skip_failed_node('s1')
    wait_for(lambda: workflow.get_failed_nodes() == [],
             message='a skipped node is no longer retryable')


def test_a_stopped_node_counts_as_a_failure(write_wf, load, fake_runner):
    fake_runner.hold = True
    workflow = load(write_wf(SERIAL))
    run_in_thread(workflow)
    wait_for(lambda: fake_runner.idents() == ['s1'])

    workflow.stop('hard')
    fake_runner.finish('s1', 'canceled')

    wait_for(lambda: workflow.get_running_status() == WorkflowStatus.FAILED,
             message='a workflow cut short is not a successful one')
    assert workflow.get_node_object('s1').get_status() == NodeStatus.STOPPED


def test_stopping_a_finished_run_does_not_reopen_it(write_wf, load, fake_runner):
    # /shutdown calls stop() on whatever is there; a workflow that already
    # ended must not be reported as stopping afterwards
    workflow = load(write_wf(SERIAL))
    run_to_completion(workflow)
    assert workflow.get_running_status() == WorkflowStatus.ENDED

    workflow.stop()

    assert workflow.get_running_status() == WorkflowStatus.ENDED
