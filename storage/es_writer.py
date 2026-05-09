import json
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers

from common.config import ELASTIC_URL

INDEX_NAME = "scored-logs"
MAPPING_PATH = Path("storage/es_index_mapping.json")

INDEX_FIELDS = [
    "log_id",
    "raw_text",
    "timestamp",
    "label",
    "final_score",
    "incident_id",
    "is_anomaly",
]


def get_client(client: Optional[Elasticsearch] = None) -> Elasticsearch:
    return client or Elasticsearch(ELASTIC_URL)


def ensure_index(client: Elasticsearch, index_name: str = INDEX_NAME) -> None:
    mapping = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name, body=mapping)


# ----------------------------
# 🔥 CORE FIX: anomaly logic
# ----------------------------
def add_anomaly_flag(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # ensure numeric
    df["final_score"] = pd.to_numeric(df["final_score"], errors="coerce").fillna(0)

    # percentile threshold (TOP 5% anomalies)
    threshold = np.percentile(df["final_score"], 95)

    df["is_anomaly"] = (
        (df["final_score"] >= threshold) |
        (df["label"].astype(str).str.lower().isin(["critical"]))
    )

    df["is_anomaly"] = df["is_anomaly"].astype(bool)

    print(f"[INFO] anomaly threshold = {threshold}")
    print(f"[INFO] anomalies count = {df['is_anomaly'].sum()}")

    return df


def index_logs(df, client=None, index_name=INDEX_NAME):
    if df is None or df.empty:
        return 0

    client = get_client(client)
    ensure_index(client, index_name=index_name)

    df = df.copy()

    # ensure all fields exist
    for field in INDEX_FIELDS:
        if field not in df.columns:
            df[field] = None

    # FIX timestamp + anomaly safety
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    actions = []

    for _, row in df.iterrows():
        doc = {
            "log_id": row["log_id"],
            "raw_text": row["raw_text"],
            "timestamp": row["timestamp"].isoformat() if pd.notnull(row["timestamp"]) else None,
            "label": row["label"],
            "final_score": float(row["final_score"]) if row["final_score"] is not None else 0.0,
            "incident_id": row["incident_id"],
            "is_anomaly": bool(row["is_anomaly"]) if row["is_anomaly"] is not None else False,
        }

        actions.append({
            "_op_type": "index",
            "_index": index_name,
            "_id": row["log_id"],
            "_source": doc,
        })

    success, _ = helpers.bulk(client, actions, refresh="wait_for")
    return success


# ----------------------------
# MAIN RUN
# ----------------------------
if __name__ == "__main__":
    from storage.db_writer import _synthetic_data

    synthetic = _synthetic_data(200)

    scored_logs = synthetic["scores"].merge(
        synthetic["logs"][["log_id", "raw_text", "timestamp"]],
        on="log_id",
        how="left",
    )

    # FIX timestamp columns safely
    if "timestamp_x" in scored_logs.columns:
        scored_logs = scored_logs.rename(columns={"timestamp_x": "timestamp"})
    if "timestamp_y" in scored_logs.columns:
        scored_logs = scored_logs.drop(columns=["timestamp_y"])

    # ----------------------------
    # 🔥 APPLY FIX HERE
    # ----------------------------
    scored_logs = add_anomaly_flag(scored_logs)

    inserted = index_logs(scored_logs)

    print(json.dumps({
        "indexed_docs": inserted,
        "index": INDEX_NAME
    }, indent=2))