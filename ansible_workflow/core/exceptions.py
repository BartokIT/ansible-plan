from enum import Enum


class ExitCodes(Enum):
    """ Define the return codes for the application"""
    VALIDATION_ERROR = 1
    VAULT_SCRIPT_ABSENT = 2
    WORKFLOW_NOT_VALID = 3
    WORKFLOW_FILE_TYPE_NOT_SUPPORTED = 4
    YAML_NOT_VALID = 5
    WORKFLOW_FAILED = 6
    PLAYBOOK_WRONG_PARAMETER = 7
    START_NODE_NOT_EXISTS = 8


class AnsibleWorkflowException(Exception):
    """ Generic Workflow Error """


# loading exceptions
class AnsibleWorkflowLoadingError(Exception):
    """ A general class for loading error """


class AnsibleWorkflowYAMLNotValid(AnsibleWorkflowLoadingError):
    """ Workflow YAML is not valid """


class AnsibleWorkflowUnsupportedVersion(AnsibleWorkflowLoadingError):
    """ Unsupported input file format version """


class AnsibleWorkflowValidationError(AnsibleWorkflowLoadingError):
    """ Unsupported content for the input file"""


class AnsibleWorkflowDuplicateNodeId(AnsibleWorkflowLoadingError):
    """ If some workflow nodes are note unique """


class AnsibleWorkflowImportErrors(AnsibleWorkflowLoadingError):
    """ General class for import errors """


class AnsibleWorkflowImportMissingBlock(AnsibleWorkflowImportErrors):
    """ If the imported file do not contains a block """


class AnsibleWorkflowRecursiveImport(AnsibleWorkflowImportErrors):
    """ If the imported file is trying to import itself """


class AnsibleWorkflowConfigurationError(AnsibleWorkflowException):
    """ Misconfiguration of Workflow """


class AnsibleWorkflowVaultScript(AnsibleWorkflowException):
    """ General vault script exception """


class AnsibleWorkflowVaultScriptNotSet(AnsibleWorkflowVaultScript):
    """ Vault script not present """


class AnsibleWorkflowVaultScriptNotExists(AnsibleWorkflowVaultScript):
    """ Vault script not present """


class AnsibleWorkflowPlaybookNodeCheck(AnsibleWorkflowConfigurationError):
    """ Playbook node check error """
