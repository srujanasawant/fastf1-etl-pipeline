# run_pipeline.py

import argparse
import json
import time

from extractors.fastf1_extractor import fetch_session, prepare_for_schema_inference
from transformers.f1_transformer import transform_fastf1_race_laps, save_parquet
from loaders.mongo_loader import store_raw
from transformers.schema_infer import register_fastf1_schema


def run_pipeline(year, race_name, session_type):
    print("\n==============================")
    print("     DYNAMIC F1 ETL RUN")
    print("==============================")
    print(f"Year: {year}")
    print(f"Race: {race_name}")
    print(f"Session: {session_type}")
    print("==============================")

    # ------------------------------------------------------
    # 1️⃣ EXTRACT
    # ------------------------------------------------------
    print("\n[1] Extracting FastF1 session data...")
    raw_payload, session = fetch_session(year, race_name, session_type)

    # ------------------------------------------------------
    # 2️⃣ PREPARE FOR SCHEMA INFERENCE
    # ------------------------------------------------------
    print("[2] Preparing schema inference input...")
    schema_input = prepare_for_schema_inference(raw_payload, session)

    print("\n\n[DEBUG] FINAL SCHEMA INPUT STRUCTURE KEYS:", schema_input.keys())
    import json
    print("[DEBUG] schema_input shape hash:", hash(json.dumps(schema_input, sort_keys=True)))


    # ------------------------------------------------------
    # 3️⃣ SCHEMA INFERENCE + REGISTRATION
    # ------------------------------------------------------
    print("[3] Inferring schema and registering...")
    schema_resp, inferred_schema = register_fastf1_schema(
        source=f"fastf1-session",
        data=schema_input
    )

    schema_id = schema_resp.get("schema_id")
    print(f"Schema Version Assigned: {schema_id}")

    # ------------------------------------------------------
    # 4️⃣ STORE RAW IN MONGODB
    # ------------------------------------------------------
    print("[4] Storing raw data in MongoDB...")
    doc_id = store_raw(
        source=f"fastf1-{session_type.lower()}",
        payload=raw_payload,
        schema_id=schema_id
    )
    print(f"Raw document stored with ID: {doc_id}")

    # ------------------------------------------------------
    # 5️⃣ TRANSFORM
    # ------------------------------------------------------
    print("[5] Transforming laps into canonical model...")
    df_laps = transform_fastf1_race_laps(raw_payload)

    # ------------------------------------------------------
    # 6️⃣ SAVE STRUCTURED DATA (PARQUET)
    # ------------------------------------------------------
    output_path = f"data/processed/laps/{year}_{race_name.replace(' ', '_')}_{session_type}.parquet"
    print(f"[6] Saving structured data to {output_path} ...")
    save_parquet(df_laps, output_path)

    print("\n==============================")
    print(" ETL PIPELINE COMPLETED ")
    print("==============================")
    print(f"Schema Version: {schema_id}")
    print(f"Raw Document ID: {doc_id}")
    print(f"Parquet Saved: {output_path}")
    print("==============================\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Dynamic F1 ETL Pipeline")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--race", type=str, required=True)
    parser.add_argument("--session", type=str, required=True)

    args = parser.parse_args()
    run_pipeline(args.year, args.race, args.session)
