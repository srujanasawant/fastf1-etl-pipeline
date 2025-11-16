# schema_registry/registry.py

import hashlib
import json
from datetime import datetime
import os
from pymongo import MongoClient, ASCENDING

# -------------------------------------------
# DB CONNECTION
# -------------------------------------------
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
client = MongoClient(MONGO_URL)
db = client["f1_etl"]

# -------------------------------------------
# INDEXES
# -------------------------------------------
db.schemas.create_index([("schema_id", ASCENDING)], unique=True)
db.schemas.create_index([("source", ASCENDING), ("version", ASCENDING)], unique=True)

# -------------------------------------------
# HELPERS
# -------------------------------------------

def fingerprint_schema(schema: dict) -> str:
    """
    Stable SHA256 fingerprint from JSON string.
    """
    s = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# -------------------------------------------
# REGISTER SCHEMA
# -------------------------------------------

def register_schema(source, schema, metadata=None):
    """
    Inserts a new schema version IF AND ONLY IF the fingerprint is new.
    Returns schema_id for the newest or existing version.
    """
    if metadata is None:
        metadata = {}

    fingerprint = fingerprint_schema(schema)

    # Check if schema with this fingerprint already exists for this source.
    existing = db.schemas.find_one(
        {"source": source, "fingerprint": fingerprint},
        {"_id": 0}
    )

    if existing:
        return existing["schema_id"]

    # Find last version:
    last = db.schemas.find_one(
        {"source": source},
        sort=[("version", -1)]
    )

    new_version = 1 if last is None else int(last["version"]) + 1

    schema_id = f"{source}-{new_version}-{fingerprint[:8]}"

    doc = {
        "schema_id": schema_id,
        "source": source,
        "version": new_version,
        "fingerprint": fingerprint,
        "schema": schema,
        "metadata": metadata,
        "created_at": datetime.utcnow()
    }

    db.schemas.insert_one(doc)
    return schema_id


# -------------------------------------------
# GETTERS
# -------------------------------------------

def get_schema_by_id(schema_id: str) -> dict:
    return db.schemas.find_one({"schema_id": schema_id}, {"_id": 0})


def get_latest_schema(source: str) -> dict:
    """
    Return highest version for this source.
    """
    return db.schemas.find_one(
        {"source": source},
        sort=[("version", -1)],
        projection={"_id": 0}
    )


def list_schemas(source: str = None, limit: int = 20) -> list:
    """
    Return list sorted by version ASCENDING so versions 1..N appear in order.
    """
    q = {}
    if source:
        q["source"] = source

    cursor = (
        db.schemas
        .find(q, {"_id": 0})
        .sort([("version", 1)])
        .limit(limit)
    )

    return list(cursor)


def find_schema_by_source_and_version(source, version):
    """
    Resolves integer or string versions safely.
    """
    try:
        version = int(version)
    except:
        pass

    return db.schemas.find_one(
        {"source": source, "version": version},
        {"_id": 0}
    )
