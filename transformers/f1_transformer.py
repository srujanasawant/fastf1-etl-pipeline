# transformers/f1_transformer.py

import pandas as pd
import numpy as np
import json
import os
from io import StringIO

from transformers.canonical_schema import canonical_f1_model

# Helper — convert time string "1:32.435" → milliseconds
def time_to_ms(value):
    """
    Converts lap time into milliseconds.
    Handles:
    - '1:32.123'
    - '92.123'
    - 92.123 (float)
    - NaN
    - timedelta
    - None
    """
    if value is None or value is np.nan:
        return np.nan

    # timedelta (FastF1 sometimes produces these)
    if isinstance(value, pd.Timedelta):
        return value.total_seconds() * 1000

    # numeric (float or int in seconds)
    if isinstance(value, (int, float, np.floating)):
        if np.isnan(value):
            return np.nan
        return float(value) * 1000

    # string values from JSON
    if isinstance(value, str):
        value = value.strip()
        if value == "" or value == "\\N":
            return np.nan

        # format "1:32.456" → convert minutes + seconds
        if ":" in value:
            mins, secs = value.split(":")
            return (float(mins) * 60 + float(secs)) * 1000

        # plain seconds string
        return float(value) * 1000

    # unknown type → skip
    return np.nan


# ----------------------------
# 1️⃣ TRANSFORM ERGAST RESULTS
# ----------------------------
def transform_ergast_results(raw_payload: dict):
    """
    Converts Ergast race results JSON into a clean flat DataFrame.
    """
    races = raw_payload["MRData"]["RaceTable"]["Races"]
    if len(races) == 0:
        return pd.DataFrame()

    race = races[0]
    season = int(race["season"])
    round_num = int(race["round"])
    race_name = race["raceName"]
    race_date = race["date"]

    rows = []
    for result in race["Results"]:
        driver = result["Driver"]
        constructor = result["Constructor"]

        row = {
            "driver_id": driver.get("code") or driver["driverId"],
            "driver_name": f"{driver['givenName']} {driver['familyName']}",
            "driver_number": driver["permanentNumber"] if "permanentNumber" in driver else None,
            "constructor": constructor["name"],

            "season": season,
            "round": round_num,
            "race_name": race_name,
            "race_date": race_date,

            "position": int(result["position"]),
            "grid": int(result["grid"]),
            "points": float(result["points"]),

            # Lap fields not present for Ergast results
            "lap_number": None,
            "lap_time_ms": None,
            "sector1_ms": None,
            "sector2_ms": None,
            "sector3_ms": None,
            "speed_trap_kph": None,
        }

        rows.append(row)

    return pd.DataFrame(rows)



# ----------------------------
# 2️⃣ TRANSFORM FASTF1 LAPS
# ----------------------------
def transform_fastf1_laps(laps_json: str, year: int, event_name: str):
    """
    laps_json is a JSON string returned by FastF1's DataFrame .to_json()
    """
    df = pd.read_json(laps_json)

    df["lap_time_ms"] = df["LapTime"].apply(time_to_ms)

    df.rename(columns={
        "Driver": "driver_id",
        "LapNumber": "lap_number",
        "Sector1Time": "sector1_ms",
        "Sector2Time": "sector2_ms",
        "Sector3Time": "sector3_ms",
        "SpeedI1": "speed_trap_kph",
    }, inplace=True, errors="ignore")

    df["season"] = year
    df["race_name"] = event_name

    return df

def transform_fastf1_race_laps(raw_payload: dict):
    """
    Takes raw FastF1 payload and transforms the laps into a canonical table.
    """
    metadata = raw_payload["metadata"]
    year = metadata["year"]
    event_name = metadata["event_name"]

    print("[Transform] Converting FastF1 laps JSON to DataFrame...")
    df = pd.read_json(StringIO(raw_payload["laps_json"]))

    # Add year + event fields
    df["season"] = year
    df["race_name"] = event_name

    # Lap time conversion
    if "LapTime" in df.columns:
        df["lap_time_ms"] = df["LapTime"].apply(time_to_ms)
    else:
        df["lap_time_ms"] = None

    # Rename columns to canonical form
    df.rename(columns={
        "Driver": "driver_id",
        "LapNumber": "lap_number",
        "Sector1Time": "sector1_ms",
        "Sector2Time": "sector2_ms",
        "Sector3Time": "sector3_ms",
        "SpeedST": "speed_trap_kph"
    }, inplace=True, errors="ignore")

    # Align canonically
    canonical_cols = list(canonical_f1_model.keys())
    for c in canonical_cols:
        if c not in df.columns:
            df[c] = None

    df = df[canonical_cols]  # reorder columns

    return df


# ----------------------------
# 3️⃣ SAVE AS PARQUET
# ----------------------------
def save_parquet(df: pd.DataFrame, output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"[Saved] {output_path}")


