import jsonschema
import json


def validate_workflow(instance, schema_path):
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    jsonschema.validate(instance=instance, schema=schema)
