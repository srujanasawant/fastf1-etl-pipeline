# loaders/mongo_loader.py
# Store raw payloads and schemas separately. 
# That satisfies requirement "store data when structure unknown upfront".
from pymongo import MongoClient
from datetime import datetime
import os

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
client = MongoClient(MONGO_URL)
db = client['f1_etl']

def store_raw(source, payload, schema_id):
    doc = {
        "source": source,
        "schema_id": schema_id,
        "payload": payload,
        "ingested_at": datetime.utcnow()
    }
    result = db.raw.insert_one(doc)
    return str(result.inserted_id)


def upsert_schema(schema_id:str, schema:dict, metadata:dict):
    db.schemas.update_one({"schema_id": schema_id},
                          {"$set": {"schema": schema, "metadata": metadata}},
                          upsert=True)
