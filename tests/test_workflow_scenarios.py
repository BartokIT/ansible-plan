'''
Whole workflows driven through the engine.

test_engine.py isolates one behaviour at a time on the smallest topology that
expresses it. This module does the opposite: it runs the shapes users actually
write - nested blocks with mixed strategies, included blocks, templated values,
checkpoints and failures inside a block - and checks what the engine makes of
them.

Still no playbook is executed; ``fake_runner`` decides the outcome of each node
and ``hold`` keeps a branch in flight so a sibling can fail underneath it.
'''
import pytest

from ansible_plan.core.models import NodeStatus, WorkflowStatus

from conftest import run_in_thread, wait_for

pytestmark = pytest.mark.usefixtures('no_svg')

# the workflow drawn in the README: blocks nested three deep, alternating
# parallel and serial strategies
NESTED = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: b1
    block:
      - id: b1.1
        import_playbook: playbooks/a.yml
      - id: b1.2
        strategy: serial
        block:
          - id: b1.2.1
            import_playbook: playbooks/b.yml
          - id: b1.2.2
            strategy: parallel
            block:
              - id: b1.2.2.1
                import_playbook: playbooks/c.yml
              - id: b1.2.2.2
                import_playbook: playbooks/d.yml
          - id: b1.2.3
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

BLOCK_WITH_CHECKPOINT = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: b1
    strategy: serial
    block:
      - id: n1
        import_playbook: playbooks/a.yml
      - id: gate
        checkpoint: true
        description: proceed with the second half?
      - id: n2
        import_playbook: playbooks/b.yml
  - id: after
    import_playbook: playbooks/c.yml
'''

TERMINAL = (WorkflowStatus.ENDED, WorkflowStatus.FAILED)


def settle(workflow, message='the run never settled'):
    wait_for(lambda: workflow.get_running_status() in TERMINAL and not workflow.is_running(),
             message=message)


def assert_ran_before(fake_runner, first, second):
    idents = fake_runner.idents()
    assert first in idents and second in idents, \
        '%s and %s should both have run, got %s' % (first, second, idents)
    assert idents.index(first) < idents.index(second), \
        '%s should have run before %s, got %s' % (first, second, idents)


# --------------------------------------------------------------------------
# nested blocks with mixed strategies
# --------------------------------------------------------------------------

def test_a_nested_workflow_runs_every_playbook(write_wf, load, fake_runner):
    workflow = load(write_wf(NESTED))
    run_in_thread(workflow)
    settle(workflow)

    assert set(fake_runner.idents()) == {
        'b1.1', 'b1.2.1', 'b1.2.2.1', 'b1.2.2.2', 'b1.2.3', 's2',
    }


def test_a_serial_block_inside_a_parallel_one_keeps_its_order(write_wf, load, fake_runner):
    workflow = load(write_wf(NESTED))
    run_in_thread(workflow)
    settle(workflow)

    # b1.2 is serial: its children run one after the other
    assert_ran_before(fake_runner, 'b1.2.1', 'b1.2.2.1')
    assert_ran_before(fake_runner, 'b1.2.1', 'b1.2.2.2')
    assert_ran_before(fake_runner, 'b1.2.2.1', 'b1.2.3')
    assert_ran_before(fake_runner, 'b1.2.2.2', 'b1.2.3')


def test_the_node_after_a_nested_block_waits_for_the_whole_subtree(write_wf, load, fake_runner):
    workflow = load(write_wf(NESTED))
    run_in_thread(workflow)
    settle(workflow)

    # s2 joins both branches of b1, the shallow one and the nested one
    assert fake_runner.idents()[-1] == 's2'
    assert_ran_before(fake_runner, 'b1.1', 's2')
    assert_ran_before(fake_runner, 'b1.2.3', 's2')


def test_the_two_branches_of_a_parallel_block_start_together(write_wf, load, fake_runner):
    fake_runner.hold = True
    workflow = load(write_wf(NESTED))
    run_in_thread(workflow)

    # b1 is parallel: the shallow branch and the first node of the nested one
    # are in flight at the same time
    wait_for(lambda: set(fake_runner.idents()) == {'b1.1', 'b1.2.1'},
             message='both branches of the outer block should have started')


def test_a_nested_parallel_pair_runs_side_by_side(write_wf, load, fake_runner):
    fake_runner.hold = True
    workflow = load(write_wf(NESTED))
    run_in_thread(workflow)
    wait_for(lambda: set(fake_runner.idents()) == {'b1.1', 'b1.2.1'})

    fake_runner.finish('b1.2.1')

    wait_for(lambda: {'b1.2.2.1', 'b1.2.2.2'} <= set(fake_runner.idents()),
             message='the inner parallel block should have started both nodes')
    assert 'b1.2.3' not in fake_runner.idents()


# --------------------------------------------------------------------------
# a failure inside a parallel block
# --------------------------------------------------------------------------

def test_a_failed_branch_lets_its_sibling_finish(write_wf, load, fake_runner):
    '''The README's rule: the block waits for the other playbook, then fails.'''
    fake_runner.hold = True
    workflow = load(write_wf(PARALLEL))
    run_in_thread(workflow)
    wait_for(lambda: set(fake_runner.idents()) == {'p1', 'p2'})

    fake_runner.finish('p1', 'failed')

    wait_for(lambda: workflow.get_node_object('p1').get_status() == NodeStatus.FAILED)
    # the sibling is left alone to complete
    assert workflow.get_node_object('p2').get_status() == NodeStatus.RUNNING

    fake_runner.finish('p2')
    wait_for(lambda: workflow.get_node_object('p2').get_status() == NodeStatus.ENDED)
    settle(workflow)

    # ... and what comes after the block is never reached
    assert 'after' not in fake_runner.idents()
    assert workflow.get_running_status() == WorkflowStatus.FAILED


def test_a_failed_branch_does_not_stop_its_sibling_from_starting(write_wf, load, fake_runner):
    fake_runner.statuses = {'p1': ['failed']}
    workflow = load(write_wf(PARALLEL))
    run_in_thread(workflow)
    settle(workflow)

    # p1 fails immediately, p2 still gets its turn
    assert set(fake_runner.idents()) == {'p1', 'p2'}
    assert workflow.get_node_object('p2').get_status() == NodeStatus.ENDED


def test_retrying_a_failed_branch_releases_the_join(write_wf, load, fake_runner):
    fake_runner.statuses = {'p1': ['failed', 'successful']}
    workflow = load(write_wf(PARALLEL))
    run_in_thread(workflow)
    settle(workflow)
    assert 'after' not in fake_runner.idents()

    workflow.restart_failed_node('p1')

    wait_for(lambda: 'after' in fake_runner.idents(),
             message='the join should be released once the branch succeeds')
    assert fake_runner.idents() == ['p1', 'p2', 'p1', 'after']


def test_skipping_a_failed_branch_releases_the_join(write_wf, load, fake_runner):
    fake_runner.statuses = {'p1': ['failed']}
    workflow = load(write_wf(PARALLEL))
    run_in_thread(workflow)
    settle(workflow)

    workflow.skip_failed_node('p1')

    wait_for(lambda: 'after' in fake_runner.idents(),
             message='a skipped branch should let the join through')
    assert workflow.get_node_object('p1').is_skipped() is True


# --------------------------------------------------------------------------
# a checkpoint inside a block
# --------------------------------------------------------------------------

def test_a_checkpoint_inside_a_block_holds_the_rest_of_the_block(write_wf, load, fake_runner):
    workflow = load(write_wf(BLOCK_WITH_CHECKPOINT))
    run_in_thread(workflow)

    wait_for(lambda: workflow.get_running_status() == WorkflowStatus.PAUSED,
             message='the checkpoint inside the block should ask')
    assert workflow.get_node_object('gate').get_status() == NodeStatus.AWAITING_CONFIRMATION
    assert fake_runner.idents() == ['n1']

    workflow.approve_node('gate')

    wait_for(lambda: fake_runner.idents() == ['n1', 'n2', 'after'],
             message='the block and the node after it should resume')


def test_refusing_a_checkpoint_inside_a_block_stops_the_branch(write_wf, load, fake_runner):
    workflow = load(write_wf(BLOCK_WITH_CHECKPOINT))
    run_in_thread(workflow)
    wait_for(lambda: workflow.get_node_object('gate').get_status() == NodeStatus.AWAITING_CONFIRMATION)

    workflow.disapprove_node('gate')

    settle(workflow)
    assert fake_runner.idents() == ['n1']
    assert 'after' not in fake_runner.idents()


# --------------------------------------------------------------------------
# included blocks actually run
# --------------------------------------------------------------------------

def test_an_included_block_is_executed(write_wf, load, fake_runner):
    write_wf('''---
id: ignored
strategy: serial
block:
  - id: i1
    import_playbook: playbooks/a.yml
  - id: i2
    import_playbook: playbooks/b.yml
''', name='_block.yml')
    workflow = load(write_wf('''---
defaults:
  inventory: inventory.ini
workflow:
  - id: inc
    include_block: _block.yml
  - id: last
    import_playbook: playbooks/c.yml
'''))
    run_in_thread(workflow)
    settle(workflow)

    assert fake_runner.idents() == ['i1', 'i2', 'last']


def test_the_same_block_included_twice_runs_twice(write_wf, load, fake_runner):
    write_wf('''---
id: ignored
block:
  - id: n
    import_playbook: playbooks/a.yml
''', name='_block.yml')
    workflow = load(write_wf('''---
defaults:
  inventory: inventory.ini
workflow:
  - id: first
    include_block: _block.yml
    id_prefix: one
  - id: second
    include_block: _block.yml
    id_prefix: two
'''))
    run_in_thread(workflow)
    settle(workflow)

    # the prefixes keep the two copies apart, so both actually run
    assert fake_runner.idents() == ['onen', 'twon']


# --------------------------------------------------------------------------
# templated values reach ansible-runner
# --------------------------------------------------------------------------

def test_templated_values_are_what_the_playbook_is_run_with(write_wf, load, fake_runner):
    workflow = load(write_wf('''---
templating:
  target: first_hostname
  suffix: prod
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    limit: "{{ target }}"
    vars:
      environment_name: "app-{{ suffix }}"
'''))
    run_in_thread(workflow)
    settle(workflow)

    call = fake_runner.call_for('n1')
    assert call['limit'] == 'first_hostname'
    assert call['extravars'] == {'environment_name': 'app-prod'}


def test_templating_overridden_per_block_reaches_each_playbook(write_wf, load, fake_runner):
    workflow = load(write_wf('''---
templating:
  who: outer
defaults:
  inventory: inventory.ini
workflow:
  - id: b1
    templating:
      who: inner
    block:
      - id: n1
        import_playbook: playbooks/a.yml
        limit: "{{ who }}"
  - id: n2
    import_playbook: playbooks/b.yml
    limit: "{{ who }}"
'''))
    run_in_thread(workflow)
    settle(workflow)

    assert fake_runner.call_for('n1')['limit'] == 'inner'
    assert fake_runner.call_for('n2')['limit'] == 'outer'


def test_command_line_templating_reaches_the_playbook(write_wf, load, fake_runner):
    workflow = load(write_wf('''---
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    limit: "{{ from_cli }}"
'''), input_templating={'from_cli': 'second_hostname'})
    run_in_thread(workflow)
    settle(workflow)

    assert fake_runner.call_for('n1')['limit'] == 'second_hostname'


# --------------------------------------------------------------------------
# partial runs on a nested workflow
# --------------------------------------------------------------------------

def test_skipping_a_node_inside_a_nested_block(write_wf, load, fake_runner):
    workflow = load(write_wf(NESTED))
    workflow.set_skipped_nodes(['b1.2.2.1'])
    run_in_thread(workflow)
    settle(workflow)

    assert 'b1.2.2.1' not in fake_runner.idents()
    # the rest of the subtree is unaffected
    assert {'b1.2.2.2', 'b1.2.3', 's2'} <= set(fake_runner.idents())


def test_starting_inside_a_nested_block_runs_that_branch(write_wf, load, fake_runner):
    workflow = load(write_wf(NESTED))
    run_in_thread(workflow, start_node='b1.2.2')
    settle(workflow)

    # what precedes the start node along its own branch is skipped
    assert workflow.get_node_object('b1.2.1').is_skipped() is True
    assert workflow.get_node_object('_s').is_skipped() is True
    assert {'b1.2.2.1', 'b1.2.2.2', 'b1.2.3'} <= set(fake_runner.idents())


def test_starting_inside_a_block_strands_the_sibling_branch(write_wf, load, fake_runner):
    """
    _set_skipped_nodes only walks back along the in edges of the start node, so
    a branch running beside it in an enclosing parallel block is neither run
    nor skipped. It stays NOT_STARTED, and since is_node_runnable accepts only
    ENDED or SKIPPED predecessors it blocks the join for good.
    """
    workflow = load(write_wf(NESTED))
    run_in_thread(workflow, start_node='b1.2.2')
    settle(workflow)

    sibling = workflow.get_node_object('b1.1')
    assert sibling.get_status() == NodeStatus.NOT_STARTED
    assert sibling.is_skipped() is False

    # so everything past the join is silently dropped
    assert workflow.get_node_object('s2').get_status() == NodeStatus.NOT_STARTED
    assert 's2' not in fake_runner.idents()


@pytest.mark.xfail(reason='known bug: a parallel branch beside the start node is '
                          'left NOT_STARTED and blocks the join, so --start-from-node '
                          'on a node inside a block silently truncates the workflow')
def test_starting_inside_a_nested_block_still_reaches_the_end(write_wf, load, fake_runner):
    workflow = load(write_wf(NESTED))
    run_in_thread(workflow, start_node='b1.2.2')
    settle(workflow)

    assert 's2' in fake_runner.idents()
