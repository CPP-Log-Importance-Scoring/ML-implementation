"""
storage/es_writer.py
====================
Indexes scored log rows into Elasticsearch so the dashboard's Log Search page
(dashboard/data/es.py::search_logs) can full-text search them.

The document schema here is kept in lock-step with what search_logs queries and
reads back:
    full-text fields : message, raw_text, template_id, host
    term filters     : host.keyword, label.keyword, template_id.keyword
    range filter     : timestamp
    returned fields  : sequence_number, timestamp, host, template_id, label,
                       importance_score, message, correlation_id

Data is assembled by joining the scored frame (labels, final_score,
correlation_id) with the sessionized frame (timestamp, host, template_id,
message) on sequence_number.

Public API
----------
index_logs(scored_df, logs_df) -> int       # bulk-index a merged batch
index_current_parquets() -> int             # one-off: index the on-disk parquets
"""

import json
from pathlib import Path

import pandas as pd
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError

from common.config import ELASTIC_URL
from common.logger import get_logger

logger = get_logger(__name__)

INDEX_NAME = "scored-logs"
MAPPING_PATH = Path("storage/es_index_mapping.json")


def get_client() -> Elasticsearch:
    return Elasticsearch(ELASTIC_URL)


def ensure_index(client, recreate: bool = False) -> None:
    """Create the index with the search-aligned mapping.

    If an index already exists but predates the current mapping (no ``host``
    field, which the search relies on), it is dropped and recreated so old
    half-baked indices can't silently break search.
    """
    mapping = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))

    if client.indices.exists(index=INDEX_NAME):
        if recreate or not _mapping_is_current(client):
            logger.info("Recreating stale Elasticsearch index '%s'.", INDEX_NAME)
            client.indices.delete(index=INDEX_NAME)
        else:
            return
    client.indices.create(index=INDEX_NAME, body=mapping)


def _mapping_is_current(client) -> bool:
    """True if the live index already has the fields the search needs."""
    try:
        live = client.indices.get_mapping(index=INDEX_NAME)
        props = live[INDEX_NAME]["mappings"].get("properties", {})
        return "host" in props and "message" in props
    except Exception:
        return False


def _build_documents(scored_df: pd.DataFrame, logs_df: pd.DataFrame) -> pd.DataFrame:
    """Join scored + sessionized rows into a single per-log document frame."""
    score_cols = [
        c for c in
        ["sequence_number", "final_score", "label", "correlation_id",
         "is_root_cause", "root_cause_confidence"]
        if c in scored_df.columns
    ]
    log_cols = [
        c for c in
        ["sequence_number", "timestamp", "host", "template_id", "message", "raw_text"]
        if c in logs_df.columns
    ]
    merged = scored_df[score_cols].merge(
        logs_df[log_cols], on="sequence_number", how="left"
    )
    return merged


def index_logs(scored_df: pd.DataFrame, logs_df: pd.DataFrame) -> int:
    """Bulk-index a scored batch into Elasticsearch. Returns docs indexed.

    Safe to call even when Elasticsearch is down — logs a warning and returns 0
    instead of raising, so it never breaks the pipeline's storage step.
    """
    try:
        client = get_client()
        ensure_index(client)
    except Exception as exc:
        logger.warning("Elasticsearch unavailable — skipping log indexing: %s", exc)
        return 0

    merged = _build_documents(scored_df, logs_df)

    def _safe_str(val) -> str:
        return "" if pd.isna(val) or val is None else str(val)

    actions = []
    for _, row in merged.iterrows():
        seq = row.get("sequence_number")
        if pd.isna(seq):
            continue
        ts = row.get("timestamp")
        log_id = f"log_{int(seq):06d}"
        doc = {
            "log_id": log_id,
            "sequence_number": int(seq),
            "timestamp": ts.isoformat() if pd.notnull(ts) else None,
            "host": _safe_str(row.get("host")),
            "template_id": _safe_str(row.get("template_id")),
            "label": _safe_str(row.get("label")),
            "message": _safe_str(row.get("message")),
            "raw_text": _safe_str(row.get("raw_text") or row.get("message")),
            "importance_score": float(row.get("final_score") or 0.0),
            "final_score": float(row.get("final_score") or 0.0),
            "correlation_id": _safe_str(row.get("correlation_id")),
            "incident_id": _safe_str(row.get("correlation_id")),
            "is_root_cause": bool(row.get("is_root_cause", False)),
            "root_cause_confidence": float(row.get("root_cause_confidence") or 0.0),
        }
        actions.append({"_index": INDEX_NAME, "_id": log_id, "_source": doc})

    if not actions:
        logger.warning("No valid documents to index into Elasticsearch.")
        return 0

    try:
        success, errors = helpers.bulk(client, actions, refresh="wait_for")
        if errors:
            logger.warning("%d document(s) failed to index.", len(errors))
        logger.info("Indexed %d log document(s) into Elasticsearch '%s'.", success, INDEX_NAME)
        return success
    except BulkIndexError as exc:
        logger.warning("Bulk indexing failed: %d doc(s) errored.", len(exc.errors))
        return 0
    except Exception as exc:
        logger.warning("Elasticsearch bulk index failed: %s", exc)
        return 0


def index_current_parquets() -> int:
    """One-off helper: index whatever is currently on disk in data/processed/."""
    import common.config as cfg

    scored = pd.read_parquet(cfg.SCORED_LOGS_PATH)
    logs = pd.read_parquet(cfg.SESSIONIZED_LOGS_PATH)
    return index_logs(scored, logs)


if __name__ == "__main__":
    print({"indexed_docs": index_current_parquets()})
