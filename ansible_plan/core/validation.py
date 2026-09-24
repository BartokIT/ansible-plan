import jsonschema
import json


def validate_workflow(instance, schema_path, base=False):
    '''
    Validate a workflow document against the schema of its format version.

    Args:
        partial (bool): The document is a base other workflows extend, so the
            keys every complete workflow must have are not required.
    '''
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    # remove all the required fields, as it is a file that must be merged
    if base:
        schema.pop('required', None)
    jsonschema.validate(instance=instance, schema=schema)
