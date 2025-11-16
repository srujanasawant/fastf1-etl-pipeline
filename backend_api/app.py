# backend_api/app.py

import numpy as np
import pandas as pd

def sanitize(obj):
    """Convert any FastF1 / Pandas / Numpy values to JSON-safe primitives."""

    # None
    if obj is None:
        return None

    # Pandas NaT
    if obj is pd.NaT:
        return None

    # NaN, Inf, -Inf
    if isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return None

    # numpy numbers → python numbers
    if isinstance(obj, (np.integer,)):
        return int(obj)

    if isinstance(obj, (np.floating,)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)

    # timestamps
    if isinstance(obj, (pd.Timestamp, np.datetime64)):
        return str(obj)

    # timedeltas
    if isinstance(obj, (pd.Timedelta, np.timedelta64)):
        return str(obj)

    # dict → recurse
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}

    # list/series/index → recurse
    if isinstance(obj, (list, tuple, set, pd.Series, pd.Index)):
        return [sanitize(x) for x in obj]

    # dataframe → dict of lists
    if isinstance(obj, pd.DataFrame):
        return {col: sanitize(obj[col].tolist()) for col in obj.columns}

    # fallback
    try:
        json.dumps(obj)  # try serializing
        return obj
    except:
        return str(obj)



from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from pymongo import MongoClient

import json

from extractors.fastf1_extractor import fetch_session
from transformers.f1_transformer import transform_fastf1_race_laps
from loaders.mongo_loader import store_raw
from schema_registry.registry import (
    get_latest_schema,
    list_schemas,
    find_schema_by_source_and_version,
)

import fastf1

app = FastAPI(title="Dynamic F1 ETL API")

# -------------------------------------
# CORS so the HTML frontend can call API
# -------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------
# Mongo client
# -------------------------------------
client = MongoClient("mongodb://localhost:27017")
db = client["f1_etl"]

# ----------------------------------------------------
# 1️⃣ FETCH SESSION (for Sessions Page & Compare Page)
# ----------------------------------------------------
@app.get("/session/fetch")
def fetch_session_api(year: int, race: str, session: str):
    try:
        raw_payload, session_obj = fetch_session(year, race, session)

        result = {
            "metadata": raw_payload["metadata"],
            "laps": json.loads(raw_payload["laps_json"]),
            "weather": json.loads(session_obj.weather_data.to_json()),
            "results": session_obj.results.to_dict() if session_obj.results is not None else None,
            "drivers": list(session_obj.drivers),
        }

        return sanitize(result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ----------------------------------------------------
# 2️⃣ COMPARE TWO SESSIONS
# ----------------------------------------------------
@app.get("/session/compare")
def compare_sessions(
    ayear: int, arace: str, asession: str,
    byear: int, brace: str, bsession: str
):
    A_raw, A = fetch_session(ayear, arace, asession)
    B_raw, B = fetch_session(byear, brace, bsession)

    summary = {
        "A_drivers": list(A.drivers),
        "B_drivers": list(B.drivers),
        "shared_drivers": list(set(A.drivers).intersection(set(B.drivers))),
        "lap_counts": {
            "A": len(A.laps),
            "B": len(B.laps)
        },
        "weather_columns": {
            "A": list(A.weather_data.columns),
            "B": list(B.weather_data.columns)
        }
    }

    return sanitize(summary)

# ----------------------------------------------------
# 3️⃣ RAW DOCUMENTS (for Raw Docs Page)
# ----------------------------------------------------
@app.get("/raw")
def get_raw_docs(source: Optional[str] = None, schema_id: Optional[str] = None):
    query = {}
    if source:
        query["source"] = source
    if schema_id:
        query["schema_id"] = schema_id

    docs = list(db.raw.find(query, {"_id":0}))
    return docs

# ----------------------------------------------------
# 4️⃣ SCHEMA LIST (already using schema service)
# ----------------------------------------------------
@app.get("/schemas")
def api_list_schemas(source: Optional[str] = None, limit: int = 20):
    return list_schemas(source, limit)

# ----------------------------------------------------
# 5️⃣ SCHEMA DIFF (wrap schema service)
# ----------------------------------------------------
from schema_registry.diff import compute_schema_diff

@app.get("/schema/diff")
def api_schema_diff(source: str, v1: int, v2: int):
    s1 = find_schema_by_source_and_version(source, v1)
    s2 = find_schema_by_source_and_version(source, v2)

    if not s1 or not s2:
        return {"error": "One or both schemas not found"}

    diff = compute_schema_diff(s1["schema"], s2["schema"])
    return {"source": source, "version_1": v1, "version_2": v2, "diff": diff}

# ----------------------------------------------------
# 6️⃣ ETL RUNS (simple mock that reads raw docs)
# ----------------------------------------------------
@app.get("/etl/runs")
def etl_runs():
    # Fake run entries based on stored raw docs
    runs = []
    docs = db.raw.find({}, {"_id":0}).sort("ingested_at", -1).limit(50)
    for d in docs:
        runs.append({
            "run_id": d.get("schema_id"),
            "source": d.get("source"),
            "schema_id": d.get("schema_id"),
            "time": str(d.get("ingested_at"))
        })
    return runs
