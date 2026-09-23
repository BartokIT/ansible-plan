'''
Tests for the two graphs the loader builds: the execution DAG that drives the
run, and the authoring hierarchy the UIs render as a tree.

The topologies asserted here are the ones drawn in the README.
'''

from ansible_plan.core.models import NodeStatus

SERIAL_TOP_LEVEL = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: s1
    import_playbook: playbooks/a.yml
  - id: s2
    import_playbook: playbooks/b.yml
'''

PARALLEL_BLOCK = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: b1
    block:
      - id: b1.1
        import_playbook: playbooks/a.yml
      - id: b1.2
        import_playbook: playbooks/b.yml
  - id: s2
    import_playbook: playbooks/c.yml
'''

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


def edges(workflow):
    return sorted(workflow.get_graph().edges())


def test_top_level_is_a_serial_chain(write_wf, load):
    assert edges(load(write_wf(SERIAL_TOP_LEVEL))) == [
        ('_s', 's1'), ('s1', 's2'), ('s2', '_e'),
    ]


def test_blocks_default_to_parallel_and_join_on_the_next_node(write_wf, load):
    assert edges(load(write_wf(PARALLEL_BLOCK))) == [
        ('_s', 'b1'),
        ('b1', 'b1.1'), ('b1', 'b1.2'),
        ('b1.1', 's2'), ('b1.2', 's2'),
        ('s2', '_e'),
    ]


def test_serial_block_chains_its_children(write_wf, load):
    workflow = PARALLEL_BLOCK.replace('    block:\n      - id: b1.1',
                                      '    strategy: serial\n    block:\n      - id: b1.1')

    assert edges(load(write_wf(workflow))) == [
        ('_s', 'b1'),
        ('b1', 'b1.1'), ('b1.1', 'b1.2'), ('b1.2', 's2'),
        ('s2', '_e'),
    ]


def test_nested_blocks_match_the_documented_graph(write_wf, load):
    assert edges(load(write_wf(NESTED))) == [
        ('_s', 'b1'),
        ('b1', 'b1.1'), ('b1', 'b1.2'),
        ('b1.1', 's2'),
        ('b1.2', 'b1.2.1'),
        ('b1.2.1', 'b1.2.2'),
        ('b1.2.2', 'b1.2.2.1'), ('b1.2.2', 'b1.2.2.2'),
        ('b1.2.2.1', 'b1.2.3'), ('b1.2.2.2', 'b1.2.3'),
        ('b1.2.3', 's2'),
        ('s2', '_e'),
    ]


def test_boundary_nodes_are_added_around_the_workflow(write_wf, load):
    graph = load(write_wf(SERIAL_TOP_LEVEL)).get_graph()

    assert list(graph.predecessors('_s')) == []
    assert list(graph.successors('_e')) == []
    assert graph.in_degree('_s') == 0


def test_hierarchy_graph_mirrors_the_file_structure(write_wf, load):
    hierarchy = load(write_wf(PARALLEL_BLOCK)).get_original_graph()

    assert sorted(hierarchy.successors('_root')) == ['_e', '_s', 'b1', 's2']
    assert sorted(hierarchy.successors('b1')) == ['b1.1', 'b1.2']
    # the hierarchy is a tree, the execution graph is not
    assert sorted(hierarchy.successors('b1.1')) == []


def test_hierarchy_edges_are_exposed_as_pairs(write_wf, load):
    loaded = load(write_wf(PARALLEL_BLOCK))

    assert ['b1', 'b1.1'] in loaded.get_original_graph_edges()


def test_block_strategy_is_attached_to_the_node_data(write_wf, load):
    loaded = load(write_wf(PARALLEL_BLOCK))

    # the UI reads 'child' to label a block with the strategy of its children
    assert loaded.get_node_datas()['b1']['child'] == {'strategy': 'parallel'}
    assert loaded.get_node_datas()['b1']['block'] == {'strategy': 'serial', 'block_id': '_root'}
    assert loaded.get_node_datas()['b1.1']['level'] == 2


# --------------------------------------------------------------------------
# validation of the assembled graph
# --------------------------------------------------------------------------

def test_a_loaded_workflow_is_valid(write_wf, load):
    assert load(write_wf(PARALLEL_BLOCK)).is_valid() is True


def test_a_cycle_makes_the_workflow_invalid(write_wf, load):
    loaded = load(write_wf(SERIAL_TOP_LEVEL))
    loaded.add_link('s2', 's1')

    assert loaded.is_valid() is False
    assert any('cyclic' in error for error in loaded.get_validation_errors())


def test_validation_collects_every_broken_node(write_wf, load):
    workflow = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/missing_one.yml
  - id: n2
    import_playbook: playbooks/missing_two.yml
'''
    loaded = load(write_wf(workflow))

    assert loaded.is_valid() is False
    assert len(loaded.get_validation_errors()) == 2


# --------------------------------------------------------------------------
# runnability and node filtering
# --------------------------------------------------------------------------

def test_a_node_is_runnable_once_its_predecessors_are_done(write_wf, load):
    loaded = load(write_wf(PARALLEL_BLOCK))

    # nothing is runnable until the _s boundary node itself is done
    assert loaded.is_node_runnable('b1') is False
    loaded.get_node_object('_s').set_status(NodeStatus.ENDED)
    assert loaded.is_node_runnable('b1') is True

    assert loaded.is_node_runnable('s2') is False

    loaded.get_node_object('b1.1').set_status(NodeStatus.ENDED)
    assert loaded.is_node_runnable('s2') is False

    loaded.get_node_object('b1.2').set_status(NodeStatus.SKIPPED)
    assert loaded.is_node_runnable('s2') is True


def test_skip_nodes_marks_exactly_those_nodes(write_wf, load):
    loaded = load(write_wf(PARALLEL_BLOCK))
    loaded.set_skipped_nodes(['b1.1'])
    loaded._set_skipped_nodes('_s', '_e')

    assert loaded.get_node_object('b1.1').is_skipped() is True
    assert loaded.get_node_object('b1.2').is_skipped() is False


def test_execute_nodes_skips_the_complement(write_wf, load):
    loaded = load(write_wf(PARALLEL_BLOCK))
    loaded.set_filtered_nodes(['b1.1'])
    loaded._set_skipped_nodes('_s', '_e')

    assert loaded.get_node_object('b1.1').is_skipped() is False
    assert loaded.get_node_object('b1.2').is_skipped() is True


def test_an_empty_filter_skips_nothing(write_wf, load):
    loaded = load(write_wf(PARALLEL_BLOCK))
    loaded.set_filtered_nodes([])
    loaded.set_skipped_nodes([])
    loaded._set_skipped_nodes('_s', '_e')

    assert [n for n in loaded.get_nodes() if loaded.get_node_object(n).is_skipped()] == []


def test_starting_later_skips_everything_upstream(write_wf, load):
    loaded = load(write_wf(NESTED))
    loaded._set_skipped_nodes('b1.2.2', '_e')

    assert loaded.get_node_object('b1.2.1').is_skipped() is True
    assert loaded.get_node_object('b1.2').is_skipped() is True
    assert loaded.get_node_object('_s').is_skipped() is True
    assert loaded.get_node_object('b1.2.2.1').is_skipped() is False
    assert loaded.get_node_object('s2').is_skipped() is False


def test_ending_earlier_skips_everything_downstream(write_wf, load):
    loaded = load(write_wf(NESTED))
    loaded._set_skipped_nodes('_s', 'b1.2.2')

    assert loaded.get_node_object('b1.2.3').is_skipped() is True
    assert loaded.get_node_object('s2').is_skipped() is True
    assert loaded.get_node_object('_e').is_skipped() is True
    assert loaded.get_node_object('b1.2.1').is_skipped() is False


def test_unknown_nodes_are_reported_as_absent(write_wf, load):
    loaded = load(write_wf(SERIAL_TOP_LEVEL))

    assert loaded.is_node_present('s1') is True
    assert loaded.is_node_present('nope') is False
