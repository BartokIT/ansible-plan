import os
import logging
import logging.handlers
import abc
import inspect
import random
import string
import typing
import jinja2
import copy
import sys
import yaml
import jsonschema
from ansible.plugins.filter.core import FilterModule
from .exceptions import (AnsibleWorkflowConfigurationError, AnsibleWorkflowImportMissingBlock, AnsibleWorkflowRecursiveImport,
                         AnsibleWorkflowUnsupportedVersion, AnsibleWorkflowValidationError,
                         AnsibleWorkflowVaultScriptNotExists,
                         AnsibleWorkflowVaultScriptNotSet, AnsibleWorkflowYAMLNotValid)
from .engine import AnsibleWorkflow
from .models import Node, PNode, BNode, INode, CNode
from .validation import validate_workflow
from collections.abc import Mapping
from enum import Enum

class WorkflowLoader(metaclass=abc.ABCMeta):
    '''
    An abstract class that need to be implemented in order to support the
    parsing of a workflow file
    '''
    @abc.abstractmethod
    def parse(self, extra_vars: typing.Dict[str, str]) -> AnsibleWorkflow:
        pass


class YamlKeys(Enum):
    META_KEY = 'meta'
    DEFAULTS_KEY = 'defaults'
    OPTIONS_KEY = 'options'
    TEMPLATING_KEY = 'templating'


class WorkflowYamlLoader(WorkflowLoader):
    '''
    Handles loading of a workflow from a yml file with directives that mimics
    Ansible task names.
    '''
    def __init__(self, workflow_file: str, logging_dir: str,
                 logging_level: str = 'error', input_templating: dict={},
                 check_mode=False, verbosity=0, doubtful_mode=False):
        '''
        Initialize the loader
        Args:
            workflow (string): The relative or absolute path to the file to
                be loaded.
            logger (logging.Logger): A logging.logger instance
        Raises:
            AnsibleWorkflowYAMLNotValid: If the file cannot be loaded or is not yaml
        Returns:
            WorkflowLoader: An instance of the abstract class WorkflowLoader
        '''
        self.__define_logger(logging_dir, logging_level)
        self.__workflow_file: str = workflow_file
        self.__workflow: AnsibleWorkflow = AnsibleWorkflow(self.__workflow_file, logging_dir, logging_level, doubtful_mode)
        self._default_format_version: int = 1
        self.__check_mode: bool = check_mode
        self.__verbosity = verbosity
        self.input_templating: dict = input_templating

        # initialize template environment
        self._template_env = jinja2.Environment(loader=jinja2.FileSystemLoader("."), undefined=jinja2.StrictUndefined)
        self._template_env.filters.update(FilterModule().filters())
        # contains the dictionary parsed from the yaml workflow file
        try:
            self.__yaml_parsed: dict = self._load_yaml(self.get_contents(self.__workflow_file))
        except yaml.scanner.ScannerError as yerr:
            self._logger.exception("Errors in YAML file format. %s" % yerr)
            raise AnsibleWorkflowYAMLNotValid(yerr.message)
        except Exception as err:
            self._logger.error("Impossible to load workflow file. %s" % err)
            raise AnsibleWorkflowYAMLNotValid(err)

    def __define_logger(self, logging_dir: str, logging_level: str):
        '''
        Define a logger specific for this class
        Args:
            logging_dir (string): The directory path where log
            logging_level (string): The level wich the logger should be set
        '''
        logger_name = self.__class__.__name__
        logger_file_path = os.path.join(logging_dir, 'loader.log')
        if not os.path.exists(os.path.dirname(logger_file_path)):
            os.makedirs(os.path.dirname(logger_file_path))

        logger = logging.getLogger(logger_name)
        if logging_level:
            logger.setLevel(getattr(logging, logging_level.upper()))
        logger_handler = logging.handlers.TimedRotatingFileHandler(
            logger_file_path,
            when='d',
            backupCount=3,
            encoding='utf8'
        )
        logger_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
        logger.addHandler(logger_handler)
        self._logger = logger
        self._logging_dir = logging_dir

    def _load_yaml(self, contents: str):
        '''
        Attempts to deserialize the contents of a YAML object
        Args:
            contents (string): The contents to deserialize
        Returns:
            dict: If the contents are YAML serialized
            None: if the contents are not YAML serialized
        '''
        return yaml.safe_load(contents)

    def _write_yaml(self, contents: dict, path: str):
        '''
        Attempts to serialize an object to a YAML string into a file
        Args:
            contents (string): The contents to write
            path (string): The path of the file
        '''
        with open(path, 'w') as file:
            yaml.dump(contents, file)

    def get_contents(self, path: str) -> str:
        '''
        Loads the contents of the file specified by path
        Args:
            path (string): The relative or absolute path to the file to
                be loaded.  If the path is relative, then it is combined
                with the base_path to generate a full path string
        Returns:
            string: The contents of the file as a string
        Raises:
            AnsibleWorkflowConfigurationError: If the file cannot be loaded
        '''
        try:
            if not os.path.exists(path):
                raise AnsibleWorkflowConfigurationError('Specified workflow path does not exist: %s' % path)

            data = ''

            prependwf_file = '{}/_wf.yml'.format(os.path.dirname(os.path.realpath(path)))

            if os.path.exists(prependwf_file):
                with open(prependwf_file) as f:
                    data += f.read()

            with open(path) as f:
                data += f.read()

            return data

        except (IOError, OSError) as err:
            self._logger.exception(err)
            raise AnsibleWorkflowConfigurationError('Error trying to load workflow file contents: %s' % err)

    def parse(self, extra_vars: typing.Dict[str, str]) -> AnsibleWorkflow:
        '''
        Parse the workflow file, checking the correctness of the format depending by the version of the format
        Args:
            extra_vars (dict): A dictionary containing the extra variables to be passed to playbooks
        Returns:
            AnsibleWorkflow: An AnsibleWorkflow class instance
        Raises:
            ValidationError: If the input file doesn't match the schema
            AnsibleWorkflowUnsupportedVersion: If the version format is not supported
        '''
        # grab the meta key to verify the version specified
        if isinstance(self.__yaml_parsed, Mapping):
            yml_format_version = self.__yaml_parsed.get(YamlKeys.META_KEY.value, {}).get('format-version', '1')
        else:
            yml_format_version = '1'

        # search a method that is able to parse this versione
        _method_name: str = '_parse_v' + str(yml_format_version)
        _method_func = getattr(self, _method_name)
        if _method_func is None:
            raise AnsibleWorkflowUnsupportedVersion

        # call the parser
        _method_func(extra_vars)
        # return the workflow instance
        return self.__workflow

    def _parse_v1(self, extra_vars: typing.Dict[str, str]):
        '''
        Parse the yaml version 1 workflow file
        Args:
            extra_vars (dict): A dictionary containing the extra variables to be passed to playbooks
        Raises:
            AnsibleWorkflowVaultScriptNotExists: If the workflow file doesn't exists
        '''
        # validate the workflow file accordingly with the format version
        schema_path = os.path.join(os.path.dirname(__file__), '..', 'schemas', 'v1.json')
        try:
            validate_workflow(self.__yaml_parsed, schema_path)
        except jsonschema.ValidationError as err:
            self._logger.fatal("Impossible to parse the workflow. Validation error: %s" % err)
            raise AnsibleWorkflowValidationError("%s" % err.message)

        options: dict = dict()
        defaults: dict = dict()
        workflow: dict = dict()
        template_variables: dict = dict()

        # get values from options and default keys of the file
        defaults['inventory'] = self.__yaml_parsed.get(YamlKeys.DEFAULTS_KEY.value, {}).get('inventory')
        defaults['verbosity'] = self.__verbosity if self.__verbosity != 0 else self.__yaml_parsed.get(YamlKeys.DEFAULTS_KEY.value, {}).get('verbosity', 0)
        defaults['vars'] = extra_vars if extra_vars else self.__yaml_parsed.get(YamlKeys.DEFAULTS_KEY.value, {}).get('vars', {})
        defaults['vault_ids'] = self.__yaml_parsed.get(YamlKeys.DEFAULTS_KEY.value, {}).get('vault_ids', [])
        defaults['project_path'] = self.__yaml_parsed.get(YamlKeys.DEFAULTS_KEY.value, {}).get('project_path', '.')
        defaults['limit'] = self.__yaml_parsed.get(YamlKeys.DEFAULTS_KEY.value, {}).get('limit')
        options['vault_script'] = self.__yaml_parsed.get(YamlKeys.OPTIONS_KEY.value, {}).get('vault_script')
        options['global_path'] = self.__yaml_parsed.get(YamlKeys.OPTIONS_KEY.value, {}).get('global_path')

        workflow_dir = os.path.dirname(os.path.abspath(self.__workflow_file))
        if options.get('global_path', False):
            if not os.path.isabs(options['global_path']):
                options['global_path'] = os.path.join(workflow_dir, options['global_path'])
        else:
            options["global_path"] = workflow_dir


        template_variables = {
            **self.input_templating,
            **self.__yaml_parsed.get(YamlKeys.TEMPLATING_KEY.value, {})
        }

        self._perform_template_rendering(template_variables, self.input_templating)
        self._perform_template_rendering(defaults, template_variables)
        self._perform_template_rendering(options, template_variables)

        # assign calculated string to be rendered
        self.__yaml_parsed[YamlKeys.TEMPLATING_KEY.value] = template_variables
        self.__yaml_parsed[YamlKeys.OPTIONS_KEY.value] = options
        self.__yaml_parsed[YamlKeys.DEFAULTS_KEY.value] = defaults

        # prepend global path to vault script
        if options.get("vault_script", False):
            if options.get("global_path", False):
                if options.get("vault_script", False) and not os.path.isabs(options["vault_script"]):
                    options["vault_script"] = os.path.join(options["global_path"], options["vault_script"])

            # calculate the absolute path for the vault script
            options["vault_script"] = os.path.abspath(options["vault_script"])
            if not os.path.exists(options["vault_script"]):
                self._logger.error("Vault script %s doesn't exists " % (options["vault_script"]))
                raise AnsibleWorkflowVaultScriptNotExists("Vault script %s doesn't exists " % (options["vault_script"]))

        # log this volues for debugging purpose
        self._logger.info("---------- [defaults] ----------")
        for default_key in defaults.keys():
            self._logger.info("%s : %s" % (default_key, defaults[default_key]))
        self._logger.info("---------- [options] ----------")
        for option_key in options.keys():
            self._logger.info("%s : %s" % (option_key, options[option_key]))
        self._logger.info("---------- [template variables] ----------")
        for template_key in template_variables.keys():
            self._logger.info("%s : %s" % (template_key, template_variables[template_key]))
        self._logger.info("-------------------------------")

        # call the creation of the workflow adding a start and end block node
        self.__yaml_parsed['workflow'].insert(0, dict(id='_s', block=[]))
        self.__yaml_parsed['workflow'].append(dict(id='_e', block=[]))

        # add a dummy root node to visualize the workflow like a tree in console
        self.__workflow.add_node(BNode("_root"),{'child': {'strategy': 'serial'}})
        self.__workflow.get_original_graph().add_node('_root')

        # perform static inclusion where include_block is found
        self._perform_static_inclusion(self.__yaml_parsed['workflow'], options, template_variables=template_variables)

        # perform templating of strings
        # self._perform_template_rendering(self.__yaml_parsed['workflow'], template_variables)
        #self._perform_template_rendering(self.__yaml_parsed, template_variables)
        self._write_yaml(self.__yaml_parsed, os.path.join(self._logging_dir, 'rendered_workflow.yml'))

        # validate twice after the file inclusion
        try:
            validate_workflow(self.__yaml_parsed, schema_path)
        except jsonschema.ValidationError as err:
            self._logger.fatal("Impossible to parse the workflow. Validation error: %s" % err)
            raise AnsibleWorkflowValidationError("Wrong workflow format.\n%s" % err.message)

        # call the parser of the workflow key, starting with a serial strategy
        self._parse_workflow_v1(to_be_imported=self.__yaml_parsed['workflow'], parent_nodes=[], strategy='serial', defaults=defaults, options=options)

    def _perform_string_template_rendering(self, template_string: str, template_variables: typing.Dict[str, typing.Any]):
        try:
            template = self._template_env.from_string(template_string)
            return template.render(**template_variables)
        except jinja2.exceptions.UndefinedError as jerr:
            self._logger.error(f"Templating error: {jerr}. Rendered block: {template_string}")
            raise jerr

    def _perform_template_rendering(self, parsed_yml: typing.Any, template_variables: typing.Dict[str, typing.Any]):
        '''
        Search inside the workflow values and apply jinja templating
        Args:
            parsed_yml (list): A list of dict containing workflow nodes in a block
            template_variables (dict): The variables to be applied to the template
        '''
        if isinstance(parsed_yml, dict):
            for key, value in parsed_yml.items():
                if isinstance(value, str):
                    parsed_yml[key] = self._perform_string_template_rendering(value, template_variables)
                    self._logger.debug("Templating: %s vars: %s" % (parsed_yml[key], template_variables))
                elif isinstance(value, dict) or isinstance(value, list):
                    self._perform_template_rendering(value, template_variables)
        elif isinstance(parsed_yml, list):
            i = 0
            for value in parsed_yml:
                if isinstance(value, str):
                    parsed_yml[i] =  self._perform_string_template_rendering(value, template_variables)
                    self._logger.debug("Templating: %s" % parsed_yml[i])
                elif isinstance(value, dict) or isinstance(value, list):
                    self._perform_template_rendering(value, template_variables)
                i = i + 1

    def _perform_static_inclusion(self, parsed_yml: typing.List[dict], options: typing.Dict[str, str], prefix='', template_variables: typing.Dict[str, typing.Any]={}):
        '''
        Perform inclusion of file containig block specifications
        Args:
            parsed_yml (list): A list of dict containing workflow nodes in a block
            defaults (dict): A dictionary with defaults settings
            prefix (str): The prefix to be added to the ids of the imported file
        Raises:
            AnsibleWorkflowImportMissingBlock: If the file imported do not contains a block directive
            AnsibleWorkflowRecursiveImport: If the imported file import the same file
        '''
        for inode in parsed_yml:
            current_template_variables = copy.copy(template_variables)
            if 'include_block' in inode:
                # perform templating for current inclusion
                temporary_template_variables = copy.copy(template_variables)
                if 'templating' in inode:
                    self._perform_template_rendering(inode['templating'], template_variables=current_template_variables)
                    temporary_template_variables.update(inode['templating'])

                # prepend included block file with global path if not absolute
                to_be_included_file = self._perform_string_template_rendering(inode.pop('include_block'), temporary_template_variables)
                included_block_prefix = inode.pop('id_prefix', '')
                if options.get("global_path", False) and not os.path.isabs(to_be_included_file):
                    to_be_included_file = os.path.join(options["global_path"], to_be_included_file)
                block_file_parsed = self._load_yaml(self.get_contents(to_be_included_file))

                # copy the identifier of the importing node
                inode_id = copy.deepcopy(inode.get('id'))
                inode_templating = copy.deepcopy(inode.get('templating'))
                # update node keys
                inode.update(block_file_parsed)

                self._logger.info("Included block file %s into the workflow" % to_be_included_file)
                # set the importing id to the imported node
                if inode_id:
                    self._logger.info("Overwrited imported block id with the importing node id (%s)" % inode_id)
                    inode["id"] = inode_id

                if 'templating' in inode:
                    current_template_variables.update(inode['templating'])

                if inode_templating:
                    self._logger.info("Overwrited imported block templating (%s) with the importing node templating (%s)" % (inode['templating'], inode_templating))
                    inode["templating"] = inode_templating
                    current_template_variables.update(inode['templating'])
                    self._logger.debug("Resulting template variables: %s" % current_template_variables)

                if 'block' not in inode:
                    self._logger.error("The imported block file %s doesn't contain a block" % to_be_included_file)
                    raise AnsibleWorkflowImportMissingBlock("The imported block file %s doesn't contain a block" % to_be_included_file)
                # check if there is an inclusion of itself
                for subnode in inode['block']:
                    if 'include_block' in subnode and subnode['include_block'] in to_be_included_file:
                        self._logger.error("The imported block file %s is importing itself" % to_be_included_file)
                        raise AnsibleWorkflowRecursiveImport("The imported block file %s is importing itself" % to_be_included_file)

                self._perform_static_inclusion(inode['block'], options, included_block_prefix, template_variables=current_template_variables)
            elif 'block' in inode:
                inode['id'] = str(prefix) + str(inode['id'])
                if 'templating' in inode:
                    current_template_variables.update(inode['templating'])
                self._perform_static_inclusion(inode['block'], options, prefix, template_variables=current_template_variables)
                # perform also a render of the remaining field of a block after the leaf node are rendered
                self._perform_template_rendering(inode, template_variables=current_template_variables)
            else:
                inode['id'] = str(prefix) + str(inode['id'])
                self._logger.info("Templating block: %s - with variables: %s" % (inode['id'], current_template_variables))
                # perform the render on the lead of a tree
                self._perform_template_rendering(inode, template_variables=current_template_variables)

    def _parse_workflow_v1(self, to_be_imported: typing.List[dict], parent_nodes: typing.List[Node],
                           strategy: str, defaults: typing.Dict[str, str],
                           options: typing.Dict[str, str], level: int = 1, block_id: str = '_root'):
        '''
        Parse the workflow key of the file, adding nodes and links to the execution graph
        Args:
            to_be_imported (list): A list of dict containing workflow nodes in a block
            parent_nodes (list): A list of Nodes instances that are parents of the actual block
            strategy (str): The strategy of the block (can be serial or parallel)
            defaults (dict): A dictionary with defaults settings
            options(dict): A dictionary with global settings
            level (int): The nesting level of the block
            block_id (str): The parent block identifier
        Raises:
            AnsibleWorkflowVaultScriptNotSet: If a node specify some vault ids but the vault script is not set
        '''
        indentation = '\t' * (len(inspect.stack(0)) - 7)

        # init to loop over the structure
        zero_outdegree_nodes = []
        for inode in to_be_imported:
            # generate a node identifier and set to the node
            gnode_id = inode.get('id', ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5)))
            gnode_id = str(gnode_id)
            inode['id'] = gnode_id

            self._logger.debug("-->> %s node: %s       parents: %s       zero_outdegree: %s" %
                               (indentation, inode['id'], [p.get_id() for p in parent_nodes], [p.get_id() for p in zero_outdegree_nodes]))

            for parent_node in parent_nodes:
                self._logger.debug("---- %s added graph link (%s) --> (%s)" % (indentation, parent_node.get_id(), gnode_id))
                self.__workflow.add_link(parent_node.get_id(), gnode_id)

            if strategy == 'serial':
                parent_nodes = []
                for zero_outdegree_node in zero_outdegree_nodes:
                    self.__workflow.add_link(zero_outdegree_node.get_id(), gnode_id)
                zero_outdegree_nodes = []

            # generate the object representing the graph
            if 'block' in inode:
                gnode = BNode(gnode_id)
                block_sub_nodes = self._parse_workflow_v1(inode['block'], [gnode, ], inode.get('strategy', 'parallel'), defaults, options, level + 1, gnode_id)
            else:
                if 'import_playbook' in inode:
                    # set the playbook
                    playbook = inode['import_playbook']
                    # init the parameters input
                    pnode_parameters = dict(id=gnode_id, playbook=playbook, artifact_dir=self._logging_dir, check_mode=self.__check_mode)

                    # set all the inode parameters or if not set defaults parameters
                    for parameter, default_or_inode_key in dict(inventory="inventory",
                                                                extra_vars="vars",
                                                                vault_ids="vault_ids",
                                                                project_path="project_path",
                                                                limit="limit",
                                                                verbosity="verbosity",
                                                                description="description",
                                                                reference="reference").items():
                        if inode.get(default_or_inode_key):
                            pnode_parameters[parameter] = inode[default_or_inode_key]
                        elif defaults.get(default_or_inode_key):
                            pnode_parameters[parameter] = defaults[default_or_inode_key]

                    # prepend global path to project and inventory
                    if options.get("global_path", False):
                        if not os.path.isabs(pnode_parameters["project_path"]):
                            pnode_parameters["project_path"] = os.path.join(options["global_path"], pnode_parameters["project_path"])
                        if not os.path.isabs(pnode_parameters["inventory"]):
                            pnode_parameters["inventory"] = os.path.join(options["global_path"], pnode_parameters["inventory"])

                    # add the vault script to all vault id
                    vault_ids = []

                    if len(pnode_parameters.get("vault_ids", [])) > 0 and options['vault_script'] is None:
                        raise AnsibleWorkflowVaultScriptNotSet("Vault script not set but vault IDs specified for node %s" % (gnode_id))

                    for vault_id in pnode_parameters.get("vault_ids", []):
                        vault_ids.append("%s@%s" % (vault_id, options["vault_script"]))
                    pnode_parameters["vault_ids"] = vault_ids

                    gnode = PNode(**pnode_parameters)
                    gnode.set_logger(self._logger)
                    self._logger.debug("---- %s added node: %s, level: %s, block type: %s, block id: %s" %
                                    (indentation, pnode_parameters, level, strategy, block_id))
                elif inode.get('checkpoint', False):
                    gnode = CNode(gnode_id, description=inode.get('description', ''), reference=inode.get('reference', ''))
                else: # It's a info node
                    gnode = INode(gnode_id, description=inode.get('description', ''), reference=inode.get('reference', ''))

            # the node specification is added
            node_info=dict(level=level, block=dict(strategy=strategy, block_id=block_id))
            if 'block' in inode:
                node_info['child']={'strategy':inode.get('strategy', 'parallel')}

            self.__workflow.add_node(gnode, node_info)

            # saves also original tree
            self.__workflow.get_original_graph().add_node(gnode_id)
            self.__workflow.get_original_graph().add_edge(block_id, gnode_id)

            if 'block' in inode:
                zero_outdegree_nodes.extend(block_sub_nodes)
            else: # This covers both playbook and dummy nodes
                if strategy == 'parallel' or (strategy == 'serial' and inode == to_be_imported[-1]):
                    zero_outdegree_nodes.append(gnode)

            # if the strategy is serial
            if strategy == 'serial':
                if 'block' in inode and len(inode['block']) > 0:
                    # add the node from the subtree as parent
                    parent_nodes = block_sub_nodes
                else:
                    # or add current node as the parent
                    parent_nodes = [gnode, ]
            self._logger.debug("<<-- %s node: %s       parents: %s       zero_outdegree: %s" %
                               (indentation, gnode_id, [p.get_id() for p in parent_nodes], [p.get_id() for p in zero_outdegree_nodes]))
        return zero_outdegree_nodes
