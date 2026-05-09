import json
from pathlib import Path
from typing import Optional

import pandas as pd
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
]


def get_client(client: Optional[Elasticsearch] = None) -> Elasticsearch:
    return client or Elasticsearch(ELASTIC_URL)


def ensure_index(client: Elasticsearch, index_name: str = INDEX_NAME) -> None:
    mapping = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name, body=mapping)


def index_logs(
    df: pd.DataFrame,
    client: Optional[Elasticsearch] = None,
    index_name: str = INDEX_NAME,
) -> int:
    if df is None or df.empty:
        return 0

    client = get_client(client)
    ensure_index(client, index_name=index_name)

    use_df = df.copy()
    for field in INDEX_FIELDS:
        if field not in use_df.columns:
            use_df[field] = None

    actions = []
    for _, row in use_df.iterrows():
        doc = {k: row[k] for k in INDEX_FIELDS}
        if isinstance(doc.get("timestamp"), pd.Timestamp):
            doc["timestamp"] = doc["timestamp"].isoformat()

        actions.append(
            {
                "_op_type": "index",
                "_index": index_name,
                "_id": str(row["log_id"]),
                "_source": doc,
            }
        )

    success, _ = helpers.bulk(client, actions, refresh="wait_for", request_timeout=60)
    return int(success)


if __name__ == "__main__":
    from storage.db_writer import _synthetic_data

    synthetic = _synthetic_data(200)
    scored_logs = synthetic["scores"].merge(
        synthetic["logs"][["log_id", "raw_text", "timestamp"]],
        on="log_id",
        how="left",
    )
    inserted = index_logs(scored_logs)
    print(json.dumps({"indexed_docs": inserted, "index": INDEX_NAME}, indent=2))
