'''
The terminal status of a run.

A workflow whose playbooks all succeed still settles on WorkflowStatus.FAILED.
``get_some_failed_task`` treats every node that is not ENDED or SKIPPED as a
failure, and two internal bookkeeping nodes are never executed:

* ``_root``, the hierarchy root the UIs render the tree from, is added to the
  execution graph but is never part of the run;
* ``_e``, the end marker, is deliberately not promoted by ``__run_step``
  (``if next_node_id != end_node``).

Both stay NOT_STARTED, so the check always reports a failure and the loop
parks on "Workflow failed, waiting for retry" instead of ending.

This was reproduced end to end with real playbooks, not only with the test
double: the backend reported "failed" for a run in which both playbooks
completed successfully.
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


def run_to_completion(workflow):
    run_in_thread(workflow)
    wait_for(lambda: workflow.get_running_status() in (WorkflowStatus.ENDED,
                                                       WorkflowStatus.FAILED)
             and not workflow.is_running())


def test_the_boundary_nodes_are_never_executed(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL))
    run_to_completion(workflow)

    assert workflow.get_node_object('s1').get_status() == NodeStatus.ENDED
    assert workflow.get_node_object('s2').get_status() == NodeStatus.ENDED
    # ... but these two never move
    assert workflow.get_node_object('_root').get_status() == NodeStatus.NOT_STARTED
    assert workflow.get_node_object('_e').get_status() == NodeStatus.NOT_STARTED


def test_unfinished_boundary_nodes_are_counted_as_failures(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL))
    run_to_completion(workflow)

    # this is the check the run loop uses to decide between ENDED and FAILED
    assert workflow.get_some_failed_task() is True


@pytest.mark.xfail(reason='known bug: _root and _e stay NOT_STARTED, so '
                          'get_some_failed_task() always reports a failure and '
                          'a fully successful workflow settles on FAILED')
def test_a_fully_successful_workflow_should_report_ended(write_wf, load, fake_runner):
    workflow = load(write_wf(SERIAL))
    run_to_completion(workflow)

    assert workflow.get_running_status() == WorkflowStatus.ENDED


def test_verify_only_is_the_one_path_that_reports_ended(write_wf, load, fake_runner):
    # verify_only sets the status directly instead of going through the loop
    workflow = load(write_wf(SERIAL))
    workflow.run(verify_only=True)

    assert workflow.get_running_status() == WorkflowStatus.ENDED
