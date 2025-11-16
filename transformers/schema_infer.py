# transformers/schema_infer.py
# genson will merge samples into a representative JSON Schema. 
# Use this to detect schema changes and create versions
from genson import SchemaBuilder    
import json
from typing import Dict
from bson import ObjectId
from pymongo import MongoClient
from genson import SchemaBuilder
import hashlib
import time
import requests
import pandas as pd
import numpy as np

def infer_jsonschema_from_samples(samples: list) -> Dict:
    """
    samples: list of dicts (JSON objects)
    returns: JSON Schema dict
    """
    builder = SchemaBuilder()
    builder.add_schema({"$schema":"http://json-schema.org/draft-07/schema#"})
    for s in samples:
        builder.add_object(s)
    return builder.to_schema()

# fingerprinting / diff helper
def schema_fingerprint(schema: dict) -> str:
    return str(hash(json.dumps(schema, sort_keys=True)))

from schema_registry.registry import register_schema

def infer_and_register(source: str, samples: list, metadata: dict = None):
    """
    Convenience: infer JSON Schema from samples and register with registry.
    Returns schema_id and schema dict.
    """
    schema = infer_jsonschema_from_samples(samples)
    schema_id = register_schema(source, schema, metadata=metadata or {"sample_count": len(samples)})
    return schema_id, schema


def infer_schema_from_dict(data: dict):
    """
    Takes a Python dict and returns a JSON schema using genson.
    """
    builder = SchemaBuilder()
    builder.add_object(data)
    return builder.to_schema()


def schema_fingerprint(schema: dict):
    """
    Creates a hash fingerprint to detect if schema changed.
    """
    raw = str(schema).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:12]

def register_fastf1_schema(source: str, data: dict):
    """
    1. Infer schema from FastF1 payload using the correct genson utility.
    2. Register schema using the direct function call (more reliable than HTTP in one repo).
    """
    # 1. Infer schema using the correct genson-based function
    inferred_schema = infer_schema_from_dict(data)

    # 2. Get metadata (optional, but good practice)
    metadata = {
        "generated_at": time.time(),
        "source_session": f"{data['metadata']['year']}-{data['metadata']['session_type']}"
    }

    # 3. Register the schema directly with the MongoDB layer
    schema_id = register_schema(source, inferred_schema, metadata=metadata)
    
    # Return the response structure expected by run_pipeline.py
    return {"schema_id": schema_id}, inferred_schema


# def register_fastf1_schema(source: str, data: dict):
#     """
#     1. Infer schema from FastF1 payload.
#     2. Generate fingerprint.
#     3. Send to Schema Registry FastAPI.
#     """
#     schema = {
#         "type": "object",
#         "properties": {},
#     }

#     def build_schema(obj):
#         if obj is None:
#             return {"type": "null"}

#         if isinstance(obj, bool):
#             return {"type": "boolean"}

#         if isinstance(obj, (int, float)):
#             return {"type": "number"}

#         if isinstance(obj, str):
#             return {"type": "string"}

#         if isinstance(obj, list):
#             if len(obj) == 0:
#                 return {"type": "array", "items": {}}
#             return {"type": "array", "items": build_schema(obj[0])}

#         if isinstance(obj, dict):
#             properties = {}
#             for k, v in obj.items():
#                 properties[k] = build_schema(v)
#             return {"type": "object", "properties": properties}

#         return {"type":"string"}  # fallback

#     schema = build_schema(data)
#     fingerprint = schema_fingerprint(schema)

#     payload = {
#         "source": source,
#         "schema": schema,
#         "metadata": {
#             "fingerprint": fingerprint,
#             "generated_at": time.time()
#         }
#     }

#     r = requests.post(
#         "http://127.0.0.1:8001/schema/register",
#         json=payload
#     )

#     if r.status_code != 200:
#         print("Schema registry error:", r.text)

#     return r.json(), schema


def sanitize_for_schema(obj):
    """
    Recursively convert all FastF1/Pandas/Numpy objects into JSON-serializable
    Python primitives so GenSON can infer a schema safely.
    """
    # Basic None
    if obj is None:
        return None

    # Pandas NaT
    if obj is pd.NaT:
        return None

    # Pandas Timestamp or numpy datetime64 → string
    if isinstance(obj, (pd.Timestamp, np.datetime64)):
        return str(obj)

    # Pandas Timedelta or numpy timedelta64 → string
    if isinstance(obj, (pd.Timedelta, np.timedelta64)):
        return str(obj)

    # numpy integer → int
    if isinstance(obj, np.integer):
        return int(obj)

    # numpy floating → float
    if isinstance(obj, np.floating):
        return float(obj)

    # NaN → None
    if isinstance(obj, float) and np.isnan(obj):
        return None

    # dict → recursively sanitize
    if isinstance(obj, dict):
        return {k: sanitize_for_schema(v) for k, v in obj.items()}

    # list → recursively sanitize
    if isinstance(obj, list):
        return [sanitize_for_schema(v) for v in obj]

    # If it's any other weird object → convert to string
    try:
        json_test = obj.__str__()
        return json_test
    except:
        return str(obj)