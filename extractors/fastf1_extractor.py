# extractors/fastf1_extractor.py
# FastF1 accesses timing and telemetry easily and returns data in Pandas-friendly form.

import os
import fastf1
import pandas as pd
import numpy as np

# Ensure cache directory exists
CACHE_DIR = 'fastf1_cache'
os.makedirs(CACHE_DIR, exist_ok=True)

fastf1.Cache.enable_cache(CACHE_DIR)

def fetch_session(year: int, event_name: str, session_type: str):
    """
    Fetch raw FastF1 session and also return the session object
    so schema inference can use weather, results, etc.
    """
    print(f"[FastF1] Loading session {year} - {event_name} - {session_type}")
    session = fastf1.get_session(year, event_name, session_type)
    session.load()

    # Raw laps as JSON
    laps_df = session.laps
    laps_json = laps_df.to_json()

    metadata = {
        "year": year,
        "event_name": event_name,
        "session_type": session_type,
        "session_name": session.name,
        "session_date": str(session.date),
        "event": session.event.get("EventName"),
        "location": session.event.get("Location"),
        "country": session.event.get("Country"),
    }

    raw_payload = {
        "metadata": metadata,
        "laps_json": laps_json
    }

    return raw_payload, session

def prepare_for_schema_inference(raw_payload: dict, session):
    """
    Build a richer structure to ensure schema drift.
    This function:
     - includes laps, weather, results, event/session info
     - includes lap column names and dtypes
     - adds schema_shape: keys named from lap columns to force structural diffs
     - sanitizes values for genson
    """
    from json import loads
    import json

    # parse laps JSON
    laps_parsed = loads(raw_payload["laps_json"])

    # lap columns and dtypes (guaranteed structural differences)
    lap_column_names = list(session.laps.columns)
    lap_column_dtypes = {col: str(dtype) for col, dtype in session.laps.dtypes.items()}

    # schema_shape: create one key per lap column (key names depend on session)
    # this turns value differences into structural (key) differences
    schema_shape = {f"lapcol__{col}": str(dtype) for col, dtype in session.laps.dtypes.items()}

    combined = {
        "metadata": raw_payload["metadata"],
        "laps": laps_parsed,

        # Weather - varies across sessions
        "weather": json.loads(session.weather_data.to_json()),

        # Results - structure varies by session type
        "results": session.results.to_dict() if session.results is not None else None,

        # Session info (compatible with FastF1 session_info)
        "session_details": {
            "session_name": session.name,
            "session_type": session.session_info.get("Type"),
            "session_key": session.session_info.get("Key"),
            "session_start": session.session_info.get("StartDate"),
            "session_end": session.session_info.get("EndDate")
        },

        # Event metadata
        "event_info": {
            "official_name": session.event.get("OfficialEventName"),
            "round_number": session.event.get("RoundNumber"),
            "location": session.event.get("Location"),
            "country": session.event.get("Country")
        },

        # guaranteed-drift keys
        "lap_column_names": lap_column_names,
        "lap_column_dtypes": lap_column_dtypes,
        "driver_list": list(session.drivers),

        # THIS is the critical structural key map — different sessions = different keys
        "schema_shape": schema_shape

    }

    # FORCE schema change structurally
    combined[f"session_marker_{raw_payload['metadata']['session_type']}"] = True
    combined[f"event_marker_{raw_payload['metadata']['event_name'].replace(' ', '_')}"] = True
    combined[f"year_marker_{raw_payload['metadata']['year']}"] = True

    # Build deterministic signature representing structure
    schema_signature = {
        "lap_columns": sorted(list(session.laps.columns)),
        "lap_dtypes": {col: str(dt) for col, dt in session.laps.dtypes.items()},
        "weather_columns": list(session.weather_data.columns),
        "has_results": session.results is not None,
        "driver_count": len(session.drivers),
        "event_official_name": session.event.get("OfficialEventName"),
        "session_code": session.name
    }

    combined["schema_signature"] = schema_signature
    combined["schema_marker"] = {
        "year": raw_payload["metadata"]["year"],
        "race": raw_payload["metadata"]["event_name"],
        "session": raw_payload["metadata"]["session_type"]
    }


    # sanitize recursively (ensure function sanitize_for_schema exists/imported)
    from transformers.schema_infer import sanitize_for_schema
    return sanitize_for_schema(combined)
