# schema_registry/diff.py
from deepdiff import DeepDiff
import json

from deepdiff import DeepDiff

def compute_schema_diff(schema1, schema2):
    diff = DeepDiff(schema1, schema2, ignore_order=True)
    return diff.to_dict()


# def diff_schemas(schema_a: dict, schema_b: dict) -> dict:
#     """
#     Returns a human-friendly diff between schema_a and schema_b.
#     Uses DeepDiff but filters noise.
#     """
#     dd = DeepDiff(schema_a, schema_b, ignore_order=True, report_repetition=True)
#     # Convert to normal dict (DeepDiff types aren't always JSON serializable)
#     return json.loads(json.dumps(dd))
