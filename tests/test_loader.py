'''
Tests for WorkflowYamlLoader: schema validation, defaults resolution, path
handling, vault wiring, static inclusion and Jinja templating.
'''
import os

import jinja2
import pytest
import yaml

from ansible_plan.core.exceptions import (
    AnsibleWorkflowConfigurationError,
    AnsibleWorkflowDuplicateNodeId,
    AnsibleWorkflowImportMissingBlock,
    AnsibleWorkflowRecursiveImport,
    AnsibleWorkflowValidationError,
    AnsibleWorkflowVaultScriptNotExists,
    AnsibleWorkflowVaultScriptNotSet,
    AnsibleWorkflowYAMLNotValid,
)
from ansible_plan.core.loader import WorkflowYamlLoader
from ansible_plan.core.models import BNode, CNode, INode, PNode

MINIMAL = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
'''


# --------------------------------------------------------------------------
# schema validation
# --------------------------------------------------------------------------

def test_minimal_workflow_loads(write_wf, load):
    workflow = load(write_wf(MINIMAL))

    assert set(workflow.get_nodes()) == {'_root', '_s', 'n1', '_e'}
    assert isinstance(workflow.get_node_object('n1'), PNode)


def test_workflow_key_is_required(write_wf, load):
    with pytest.raises(AnsibleWorkflowValidationError):
        load(write_wf('---\ndefaults:\n  inventory: inventory.ini\n'))


def test_unknown_top_level_key_is_rejected(write_wf, load):
    # the schema sets additionalProperties: false at every level
    with pytest.raises(AnsibleWorkflowValidationError):
        load(write_wf(MINIMAL + 'unexpected: value\n'))


def test_unknown_node_key_is_rejected(write_wf, load):
    workflow = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    not_a_real_key: 1
'''
    with pytest.raises(AnsibleWorkflowValidationError):
        load(write_wf(workflow))


def test_broken_yaml_is_reported_as_such(write_wf, load):
    with pytest.raises(AnsibleWorkflowYAMLNotValid):
        load(write_wf('---\nworkflow:\n  - id: n1\n   import_playbook: "\n'))


def test_missing_workflow_file_is_reported(load, workdir):
    with pytest.raises(AnsibleWorkflowYAMLNotValid):
        load(str(workdir / 'does_not_exist.yml'))


def test_format_version_dispatches_by_method_name(write_wf, load):
    # parse() looks up _parse_v<version>; there is no _parse_v2, and the
    # AttributeError from getattr() beats the intended
    # AnsibleWorkflowUnsupportedVersion, which is therefore unreachable.
    workflow = '---\nmeta:\n  format-version: 2\n' + MINIMAL[4:]
    with pytest.raises(AttributeError):
        load(write_wf(workflow))


# --------------------------------------------------------------------------
# node identifiers
# --------------------------------------------------------------------------

def test_duplicate_node_id_is_rejected(write_wf, load):
    workflow = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: same
    import_playbook: playbooks/a.yml
  - id: same
    import_playbook: playbooks/b.yml
'''
    with pytest.raises(AnsibleWorkflowDuplicateNodeId, match='already present'):
        load(write_wf(workflow))


@pytest.mark.parametrize('reserved', ['_s', '_e', '_root'])
def test_reserved_node_ids_are_rejected(write_wf, load, reserved):
    workflow = MINIMAL.replace('id: n1', 'id: %s' % reserved)
    with pytest.raises(AnsibleWorkflowDuplicateNodeId, match='reserved'):
        load(write_wf(workflow))


def test_comma_in_node_id_is_rejected(write_wf, load):
    # node ids travel through comma separated CLI options (--skip-nodes)
    workflow = MINIMAL.replace('id: n1', "id: 'a,b'")
    with pytest.raises(AnsibleWorkflowDuplicateNodeId, match='unallowed characters'):
        load(write_wf(workflow))


def test_missing_id_gets_generated(write_wf, load):
    workflow = '''---
defaults:
  inventory: inventory.ini
workflow:
  - import_playbook: playbooks/a.yml
'''
    generated = set(load(write_wf(workflow)).get_nodes()) - {'_root', '_s', '_e'}

    assert len(generated) == 1
    assert len(generated.pop()) == 5


def test_a_generated_id_is_prefixed_like_any_other(write_wf, load):
    write_wf("""---
id: ignored
block:
  - import_playbook: playbooks/a.yml
""", name='_block.yml')
    workflow = """---
defaults:
  inventory: inventory.ini
workflow:
  - id: inc
    include_block: _block.yml
    id_prefix: sub
"""
    generated = set(load(write_wf(workflow)).get_nodes()) - {'_root', '_s', '_e', 'inc'}

    assert len(generated) == 1
    assert generated.pop().startswith('sub')


def test_numeric_ids_are_coerced_to_strings(write_wf, load):
    workflow = MINIMAL.replace('id: n1', 'id: 42')

    assert '42' in load(write_wf(workflow)).get_nodes()


# --------------------------------------------------------------------------
# node kinds
# --------------------------------------------------------------------------

def test_node_kinds_are_derived_from_the_keys_used(write_wf, load):
    workflow = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: info
    description: just a marker
  - id: gate
    checkpoint: true
    description: wait for me
  - id: grouping
    block:
      - id: play
        import_playbook: playbooks/a.yml
'''
    loaded = load(write_wf(workflow))

    assert isinstance(loaded.get_node_object('info'), INode)
    assert isinstance(loaded.get_node_object('gate'), CNode)
    assert isinstance(loaded.get_node_object('grouping'), BNode)
    assert isinstance(loaded.get_node_object('play'), PNode)


def test_description_and_reference_are_carried_over(write_wf, load):
    workflow = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    description: does the thing
    reference: LINUX TEAM
'''
    node = load(write_wf(workflow)).get_node_object('n1')

    assert node.get_description() == 'does the thing'
    assert node.get_reference() == 'LINUX TEAM'


# --------------------------------------------------------------------------
# defaults and per node overrides
# --------------------------------------------------------------------------

def test_defaults_are_applied_to_every_node(write_wf, load, workdir):
    workflow = '''---
defaults:
  inventory: inventory.ini
  limit: first_hostname
  vars:
    shared: value
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
  - id: n2
    import_playbook: playbooks/b.yml
'''
    loaded = load(write_wf(workflow))

    for node_id in ('n1', 'n2'):
        node = loaded.get_node_object(node_id)
        assert node.get_inventory() == str(workdir / 'inventory.ini')
        assert node.get_extravars() == {'shared': 'value'}


def test_node_values_override_defaults(write_wf, load, workdir):
    workflow = '''---
defaults:
  inventory: inventory.ini
  vars:
    shared: value
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    inventory: alternative_inventory.ini
    vars:
      only_mine: 1
'''
    node = load(write_wf(workflow)).get_node_object('n1')

    assert node.get_inventory() == str(workdir / 'alternative_inventory.ini')
    assert node.get_extravars() == {'only_mine': 1}


def test_command_line_extra_vars_replace_the_default_vars(write_wf, load):
    # note: extra_vars do not merge into defaults.vars, they take their place
    workflow = '''---
defaults:
  inventory: inventory.ini
  vars:
    from_file: yes
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
'''
    node = load(write_wf(workflow), extra_vars={'from_cli': 'x'}).get_node_object('n1')

    assert node.get_extravars() == {'from_cli': 'x'}


def test_command_line_verbosity_wins_over_the_default(write_wf, load):
    workflow = MINIMAL.replace('  inventory: inventory.ini',
                               '  inventory: inventory.ini\n  verbosity: 2')

    assert load(write_wf(workflow)).get_node_object('n1').get_verbosity() == 2
    assert load(write_wf(workflow), verbosity=4).get_node_object('n1').get_verbosity() == 4


# --------------------------------------------------------------------------
# path resolution
# --------------------------------------------------------------------------

def test_global_path_defaults_to_the_workflow_directory(write_wf, load, workdir):
    loaded = load(write_wf(MINIMAL, name='nested/deep.yml'))
    # inventory.ini lives beside the fixture skeleton, not beside the workflow
    # file, so resolving against the workflow directory must fail validation
    assert loaded.is_valid() is False
    assert any('inventory' in error for error in loaded.get_validation_errors())
    assert str(workdir / 'nested') in loaded.get_validation_errors()[0]


def test_relative_global_path_is_resolved_against_the_workflow_file(write_wf, load, workdir):
    workflow = '''---
options:
  global_path: ..
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
'''
    loaded = load(write_wf(workflow, name='nested/deep.yml'))

    assert loaded.is_valid() is True
    assert loaded.get_node_object('n1').get_inventory() == str(workdir / 'inventory.ini')


def test_absolute_paths_are_left_alone(write_wf, load, workdir):
    workflow = '''---
defaults:
  inventory: %s
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
''' % (workdir / 'inventory.ini')
    loaded = load(write_wf(workflow, name='nested/deep.yml'))

    assert loaded.get_node_object('n1').get_inventory() == str(workdir / 'inventory.ini')


# --------------------------------------------------------------------------
# vault wiring
# --------------------------------------------------------------------------

def test_vault_ids_are_paired_with_the_vault_script(write_wf, load, workdir):
    workflow = '''---
options:
  vault_script: vault_script.py
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    vault_ids:
      - ID1
      - ID2
'''
    load(write_wf(workflow))
    node = load(write_wf(workflow)).get_node_object('n1')
    script = str(workdir / 'vault_script.py')

    assert node._PNode__vault_ids == ['ID1@%s' % script, 'ID2@%s' % script]


def test_vault_ids_without_a_vault_script_are_rejected(write_wf, load):
    workflow = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    vault_ids:
      - ID1
'''
    with pytest.raises(AnsibleWorkflowVaultScriptNotSet):
        load(write_wf(workflow))


def test_missing_vault_script_is_rejected(write_wf, load):
    workflow = '''---
options:
  vault_script: nope.py
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
'''
    with pytest.raises(AnsibleWorkflowVaultScriptNotExists):
        load(write_wf(workflow))


# --------------------------------------------------------------------------
# static inclusion
# --------------------------------------------------------------------------

INCLUDED_BLOCK = '''---
id: ignored
strategy: parallel
block:
  - id: i1
    import_playbook: playbooks/a.yml
  - id: i2
    import_playbook: playbooks/b.yml
'''


def test_included_block_is_expanded_in_place(write_wf, load):
    write_wf(INCLUDED_BLOCK, name='_block.yml')
    workflow = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: inc
    include_block: _block.yml
'''
    loaded = load(write_wf(workflow))

    # the importing id replaces the one declared inside the included file
    assert 'ignored' not in loaded.get_nodes()
    assert set(loaded.get_nodes()) == {'_root', '_s', 'inc', 'i1', 'i2', '_e'}
    assert sorted(loaded.get_original_graph().successors('inc')) == ['i1', 'i2']


def test_id_prefix_allows_including_the_same_file_twice(write_wf, load):
    write_wf(INCLUDED_BLOCK, name='_block.yml')
    workflow = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: first
    include_block: _block.yml
    id_prefix: one
  - id: second
    include_block: _block.yml
    id_prefix: two
'''
    nodes = set(load(write_wf(workflow)).get_nodes())

    assert {'onei1', 'onei2', 'twoi1', 'twoi2'} <= nodes


def test_included_file_without_a_block_is_rejected(write_wf, load):
    write_wf('---\nid: x\nstrategy: parallel\n', name='_block.yml')
    workflow = MINIMAL.replace('  - id: n1\n    import_playbook: playbooks/a.yml',
                               '  - id: inc\n    include_block: _block.yml')

    with pytest.raises(AnsibleWorkflowImportMissingBlock):
        load(write_wf(workflow))


def test_self_inclusion_is_rejected(write_wf, load):
    write_wf('---\nid: x\nblock:\n  - id: y\n    include_block: _block.yml\n',
             name='_block.yml')
    workflow = MINIMAL.replace('  - id: n1\n    import_playbook: playbooks/a.yml',
                               '  - id: inc\n    include_block: _block.yml')

    with pytest.raises(AnsibleWorkflowRecursiveImport):
        load(write_wf(workflow))


def test_nested_inclusion_is_followed(write_wf, load):
    write_wf('''---
id: outer
block:
  - id: o1
    import_playbook: playbooks/a.yml
  - id: o2
    include_block: _inner.yml
''', name='_outer.yml')
    write_wf('''---
id: inner
block:
  - id: n_1
    import_playbook: playbooks/b.yml
''', name='_inner.yml')
    workflow = MINIMAL.replace('  - id: n1\n    import_playbook: playbooks/a.yml',
                               '  - id: inc\n    include_block: _outer.yml')

    assert {'o1', 'o2', 'n_1'} <= set(load(write_wf(workflow)).get_nodes())


# --------------------------------------------------------------------------
# templating
# --------------------------------------------------------------------------

def test_template_variables_are_rendered_into_node_values(write_wf, load):
    workflow = '''---
templating:
  target: first_hostname
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    limit: "{{ target }}"
'''
    node = load(write_wf(workflow)).get_node_object('n1')

    assert node._PNode__limit == 'first_hostname'


def test_input_templating_from_the_command_line_is_available(write_wf, load):
    workflow = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    limit: "{{ from_cli }}"
'''
    loaded = load(write_wf(workflow), input_templating={'from_cli': 'second_hostname'})

    assert loaded.get_node_object('n1')._PNode__limit == 'second_hostname'


def test_file_templating_wins_over_the_command_line(write_wf, load):
    workflow = '''---
templating:
  shared: from_file
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    limit: "{{ shared }}"
'''
    loaded = load(write_wf(workflow), input_templating={'shared': 'from_cli'})

    assert loaded.get_node_object('n1')._PNode__limit == 'from_file'


def test_block_templating_overrides_the_outer_value(write_wf, load):
    workflow = '''---
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
'''
    loaded = load(write_wf(workflow))

    assert loaded.get_node_object('n1')._PNode__limit == 'inner'
    assert loaded.get_node_object('n2')._PNode__limit == 'outer'


def test_templating_on_the_including_node_overrides_the_included_file(write_wf, load):
    write_wf('''---
id: ignored
templating:
  who: from_included_file
block:
  - id: i1
    import_playbook: playbooks/a.yml
    limit: "{{ who }}"
''', name='_block.yml')
    workflow = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: inc
    include_block: _block.yml
    templating:
      who: from_importing_node
'''
    loaded = load(write_wf(workflow))

    assert loaded.get_node_object('i1')._PNode__limit == 'from_importing_node'


def test_undefined_template_variables_are_an_error(write_wf, load):
    workflow = '''---
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    limit: "{{ never_defined }}"
'''
    with pytest.raises(jinja2.exceptions.UndefinedError):
        load(write_wf(workflow))


def test_ansible_filters_are_available_in_templates(write_wf, load):
    # the loader registers ansible's own filter plugins on the jinja env
    workflow = '''---
templating:
  host: first-hostname
defaults:
  inventory: inventory.ini
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    limit: "{{ host | regex_replace('-', '_') }}"
'''
    assert load(write_wf(workflow)).get_node_object('n1')._PNode__limit == 'first_hostname'


# --------------------------------------------------------------------------
# side files
# --------------------------------------------------------------------------

def test_wf_yml_beside_the_workflow_is_its_base(write_wf, load, workdir):
    write_wf('defaults:\n  inventory: inventory.ini\n  limit: first_hostname\n',
             name='_wf.yml')
    workflow = 'workflow:\n  - id: n1\n    import_playbook: playbooks/a.yml\n'
    loaded = load(write_wf(workflow))

    assert loaded.get_node_object('n1').get_inventory() == str(workdir / 'inventory.ini')
    assert loaded.get_node_object('n1')._PNode__limit == 'first_hostname'


# --------------------------------------------------------------------------
# workflow bases: _wf.yml and meta.extends
# --------------------------------------------------------------------------

ONE_NODE = '''workflow:
  - id: n1
    import_playbook: playbooks/a.yml
'''


def _limit(workflow):
    return workflow.get_node_object('n1')._PNode__limit


def test_a_base_is_merged_key_by_key(write_wf, load, workdir):
    # both files declare defaults: the base still provides the inventory
    write_wf('---\ndefaults:\n  inventory: inventory.ini\n  limit: from_base\n', name='_wf.yml')
    loaded = load(write_wf('---\ndefaults:\n  limit: from_workflow\n' + ONE_NODE))

    assert loaded.get_node_object('n1').get_inventory() == str(workdir / 'inventory.ini')
    assert _limit(loaded) == 'from_workflow'


def test_files_starting_with_a_document_marker_can_be_merged(write_wf, load):
    # text concatenation turned these into two YAML documents
    write_wf('---\ndefaults:\n  inventory: inventory.ini\n', name='_wf.yml')

    assert set(load(write_wf('---\n' + ONE_NODE)).get_nodes()) == {'_root', '_s', 'n1', '_e'}


def test_nested_mappings_are_merged_and_lists_replaced(write_wf, load):
    write_wf('''---
templating:
  kept: from_base
  replaced: from_base
defaults:
  inventory: inventory.ini
  vars:
    base_var: 1
workflow:
  - id: from_base
    import_playbook: playbooks/b.yml
''', name='_wf.yml')
    loaded = load(write_wf('''---
templating:
  replaced: from_workflow
defaults:
  vars:
    workflow_var: 2
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    limit: "{{ kept }} {{ replaced }}"
'''))

    node = loaded.get_node_object('n1')
    assert _limit(loaded) == 'from_base from_workflow'
    assert node.get_extravars() == {'base_var': 1, 'workflow_var': 2}
    # the workflow list is replaced, not appended to
    assert 'from_base' not in loaded.get_nodes()


def test_a_base_can_provide_the_whole_workflow(write_wf, load):
    write_wf('---\ndefaults:\n  inventory: inventory.ini\n' + ONE_NODE, name='_wf.yml')
    loaded = load(write_wf('---\ndefaults:\n  limit: only_this\n'))

    assert _limit(loaded) == 'only_this'


def test_meta_extends_names_the_base_relative_to_the_declaring_file(write_wf, load, workdir):
    write_wf('---\ndefaults:\n  inventory: inventory.ini\n  limit: shared\n', name='common/base.yml')
    loaded = load(write_wf('---\nmeta:\n  extends: common/base.yml\n' + ONE_NODE))

    assert _limit(loaded) == 'shared'


def test_meta_extends_takes_the_place_of_the_implicit_base(write_wf, load):
    write_wf('---\ndefaults:\n  inventory: inventory.ini\n  limit: implicit\n', name='_wf.yml')
    write_wf('---\ndefaults:\n  inventory: inventory.ini\n  limit: explicit\n', name='explicit.yml')
    loaded = load(write_wf('---\nmeta:\n  extends: explicit.yml\n' + ONE_NODE))

    assert _limit(loaded) == 'explicit'


def test_meta_extends_null_opts_out_of_the_implicit_base(write_wf, load):
    write_wf('---\ndefaults:\n  limit: implicit\n', name='_wf.yml')
    loaded = load(write_wf('---\nmeta:\n  extends: null\ndefaults:\n  inventory: inventory.ini\n' + ONE_NODE))

    assert _limit(loaded) is None


def test_bases_can_be_chained(write_wf, load):
    write_wf('---\ndefaults:\n  inventory: inventory.ini\n  limit: root\ntemplating:\n  who: root\n',
             name='common/root.yml')
    # a base extends through meta.extends only: this _wf.yml is not applied
    write_wf('---\ndefaults:\n  limit: not_applied\n', name='common/_wf.yml')
    write_wf('---\nmeta:\n  extends: root.yml\ntemplating:\n  who: middle\n', name='common/middle.yml')
    loaded = load(write_wf('''---
meta:
  extends: common/middle.yml
workflow:
  - id: n1
    import_playbook: playbooks/a.yml
    description: "{{ who }}"
'''))

    assert _limit(loaded) == 'root'
    assert loaded.get_node_object('n1').get_description() == 'middle'


@pytest.mark.parametrize('files', [
    {'workflow.yml': 'workflow.yml'},
    {'workflow.yml': 'a.yml', 'a.yml': 'b.yml', 'b.yml': 'a.yml'},
])
def test_a_workflow_extending_itself_is_rejected(write_wf, load, files):
    paths = {name: write_wf('---\nmeta:\n  extends: %s\ndefaults:\n  inventory: inventory.ini\n%s' % (base, ONE_NODE),
                            name=name)
             for name, base in files.items()}

    with pytest.raises(AnsibleWorkflowRecursiveImport):
        load(paths['workflow.yml'])


def test_a_missing_base_is_reported_with_its_path(write_wf, load):
    with pytest.raises(AnsibleWorkflowYAMLNotValid, match='missing.yml'):
        load(write_wf('---\nmeta:\n  extends: missing.yml\n' + ONE_NODE))


def test_an_invalid_base_is_reported_with_its_path(write_wf, load):
    write_wf('---\nunexpected: value\n', name='_wf.yml')

    with pytest.raises(AnsibleWorkflowValidationError, match='_wf.yml'):
        load(write_wf('---\ndefaults:\n  inventory: inventory.ini\n' + ONE_NODE))


def test_a_base_that_is_not_a_mapping_is_rejected(write_wf, load):
    write_wf('---\n- just\n- a list\n', name='_wf.yml')

    with pytest.raises(AnsibleWorkflowValidationError, match='mapping'):
        load(write_wf('---\ndefaults:\n  inventory: inventory.ini\n' + ONE_NODE))


def test_broken_yaml_in_a_base_names_the_base(write_wf, load):
    write_wf('---\ndefaults:\n  inventory: "\n', name='_wf.yml')

    with pytest.raises(AnsibleWorkflowYAMLNotValid, match='_wf.yml'):
        load(write_wf('---\n' + ONE_NODE))


def test_a_workflow_named_wf_yml_does_not_extend_itself(write_wf, load):
    loaded = load(write_wf('---\ndefaults:\n  inventory: inventory.ini\n' + ONE_NODE, name='_wf.yml'))

    assert 'n1' in loaded.get_nodes()


def test_included_files_do_not_get_the_implicit_base(write_wf, load):
    # _wf.yml used to be prepended to included files too, pushing workflow
    # level keys into the including block
    write_wf('---\ndefaults:\n  inventory: inventory.ini\n', name='_wf.yml')
    write_wf(INCLUDED_BLOCK, name='_block.yml')
    loaded = load(write_wf('---\nworkflow:\n  - id: inc\n    include_block: _block.yml\n'))

    assert {'i1', 'i2'} <= set(loaded.get_nodes())


def test_the_rendered_workflow_shows_the_merged_document(write_wf, load, log_dir):
    write_wf('---\ndefaults:\n  inventory: inventory.ini\n  limit: from_base\n', name='base.yml')
    load(write_wf('---\nmeta:\n  extends: base.yml\n' + ONE_NODE))

    rendered = yaml.safe_load(open(os.path.join(log_dir, 'rendered_workflow.yml')))
    assert rendered['defaults']['limit'] == 'from_base'
    assert 'extends' not in rendered.get('meta', {})


def test_rendered_workflow_is_written_to_the_log_directory(write_wf, load, log_dir):
    write_wf(INCLUDED_BLOCK, name='_block.yml')
    workflow = '''---
templating:
  who: rendered
defaults:
  inventory: inventory.ini
workflow:
  - id: inc
    include_block: _block.yml
  - id: n1
    import_playbook: playbooks/a.yml
    limit: "{{ who }}"
'''
    load(write_wf(workflow))

    rendered = yaml.safe_load(open(os.path.join(log_dir, 'rendered_workflow.yml')))
    ids = [node['id'] for node in rendered['workflow']]

    # inclusion and templating are already resolved in the dumped copy
    assert ids == ['_s', 'inc', 'n1', '_e']
    assert rendered['workflow'][2]['limit'] == 'rendered'


def test_loader_writes_its_own_log_file(write_wf, log_dir):
    WorkflowYamlLoader(write_wf(MINIMAL), log_dir, 'debug').parse({})

    assert os.path.exists(os.path.join(log_dir, 'loader.log'))


def test_unreadable_include_is_reported(write_wf, load):
    workflow = MINIMAL.replace('  - id: n1\n    import_playbook: playbooks/a.yml',
                               '  - id: inc\n    include_block: missing.yml')

    with pytest.raises(AnsibleWorkflowConfigurationError):
        load(write_wf(workflow))
