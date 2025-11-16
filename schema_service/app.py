# schema_service/app.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict
from schema_registry.registry import (
    register_schema,
    get_schema_by_id,
    get_latest_schema,
    list_schemas,
    find_schema_by_source_and_version
)
from schema_registry.diff import compute_schema_diff
from fastapi import Query

app = FastAPI(title="Schema Registry Service - Dynamic F1 ETL")

class SchemaPayload(BaseModel):
    source: str
    schema: Dict[str, Any]
    metadata: Dict[str, Any] = {}

@app.post("/schema/register")
def post_register(payload: SchemaPayload):
    sid = register_schema(payload.source, payload.schema, metadata=payload.metadata)
    return {"schema_id": sid}

@app.get("/schema/latest/{source}")
def get_latest(source: str):
    s = get_latest_schema(source)
    if not s:
        raise HTTPException(status_code=404, detail="No schema found for source")
    return s

@app.get("/schema/{schema_id}")
def get_by_id(schema_id: str):
    s = get_schema_by_id(schema_id)
    if not s:
        raise HTTPException(status_code=404, detail="schema not found")
    return s

@app.get("/schemas")
def get_list(source: str = None, limit: int = 20):
    return list_schemas(source, limit)

@app.get("/schema/diff/{schema_id_a}/{schema_id_b}")
def get_diff(schema_id_a: str, schema_id_b: str):
    a = get_schema_by_id(schema_id_a)
    b = get_schema_by_id(schema_id_b)
    if not a or not b:
        raise HTTPException(status_code=404, detail="schema id(s) not found")

    diff = compute_schema_diff(a["schema"], b["schema"])
    return {"diff": diff}


@app.get("/schema/diff")
def diff_by_version(source: str, v1: int = Query(...), v2: int = Query(...)):
    """
    Compare two schema versions for a given source.
    Example:
    /schema/diff?source=fastf1-r&v1=1&v2=2
    """ 
    s1 = find_schema_by_source_and_version(source, v1)
    s2 = find_schema_by_source_and_version(source, v2)

    if not s1 or not s2:
        return {"error": "One or both schemas not found"}

    diff = compute_schema_diff(s1["schema"], s2["schema"])

    return {
        "source": source,
        "version_1": v1,
        "version_2": v2,
        "diff": diff
    }