"""
dashboard/data/es.py
====================
Elasticsearch query helpers for the HPE CX Incident Intelligence Dashboard.

All functions are wrapped in try/except — if ES is down, return [] and log warning.
Uses a module-level cached client to avoid reconnecting on every call.
"""

from __future__ import annotations

from common.config import ELASTIC_URL
from common.logger import get_logger

logger = get_logger(__name__)

INDEX_NAME = "scored-logs"

# Module-level cached client — avoids a new TCP connection per search call.
_ES_CLIENT = None


def _get_client():
    """Return a cached Elasticsearch client, creating one if needed."""
    global _ES_CLIENT
    if _ES_CLIENT is None:
        try:
            from elasticsearch import Elasticsearch
            _ES_CLIENT = Elasticsearch(
                ELASTIC_URL,
                request_timeout=10,
                max_retries=2,
                retry_on_timeout=True,
            )
        except Exception as exc:
            logger.warning("Failed to create Elasticsearch client: %s", exc)
            return None
    return _ES_CLIENT


def _safe_term(field: str, value: str) -> dict:
    return {"term": {field: {"value": value}}}


def _latest_timestamp(client) -> str | None:
    """Return the max ``timestamp`` in the index as an ISO string, or None.

    Search windows are anchored to the most recent indexed log rather than
    wall-clock ``now`` — otherwise replayed / synthetic demo data whose
    timestamps don't line up with the current clock (e.g. future-dated July
    logs viewed in June) falls entirely outside ``[now-Nh, now]`` and search
    silently returns nothing.
    """
    try:
        resp = client.search(
            index=INDEX_NAME,
            size=0,
            aggs={"max_ts": {"max": {"field": "timestamp"}}},
        )
        return resp.get("aggregations", {}).get("max_ts", {}).get("value_as_string")
    except Exception as exc:
        logger.warning("Could not resolve latest indexed timestamp: %s", exc)
        return None


def _time_filter(client, time_range_hours: int) -> dict:
    """Build a timestamp range filter for the last ``time_range_hours``.

    Anchored to the newest indexed log when available (so replayed data is
    searchable), falling back to wall-clock ``now`` for a live/empty index.
    """
    hours = int(time_range_hours)
    anchor = _latest_timestamp(client)
    if anchor:
        return {"range": {"timestamp": {"gte": f"{anchor}||-{hours}h", "lte": anchor}}}
    return {"range": {"timestamp": {"gte": f"now-{hours}h", "lte": "now"}}}


def search_logs(
    query: str,
    host: str | None = None,
    label: str | None = None,
    template_id: str | None = None,
    time_range_hours: int = 24,
    size: int = 100,
) -> list[dict]:
    """
    Full-text search across log messages.

    Returns list of dicts with keys:
        sequence_number, timestamp, host, template_id,
        label, importance_score, message, correlation_id
    """
    try:
        client = _get_client()
        if client is None:
            return []

        must_clauses: list[dict] = []

        if query and query.strip():
            must_clauses.append(
                {
                    "multi_match": {
                        "query": query.strip(),
                        "fields": [
                            "message^2",
                            "raw_text",
                            "template_id^1.5",
                            "host",
                        ],
                        "type": "best_fields",
                        "fuzziness": "AUTO",
                    }
                }
            )

        if host is not None:
            must_clauses.append(_safe_term("host.keyword", host))
        if label is not None:
            must_clauses.append(_safe_term("label.keyword", label))
        if template_id is not None:
            must_clauses.append(_safe_term("template_id.keyword", template_id))

        if not must_clauses:
            must_clauses.append({"match_all": {}})

        query_body = {
            "bool": {
                "must": must_clauses,
                "filter": [_time_filter(client, time_range_hours)],
            }
        }

        response = client.search(
            index=INDEX_NAME,
            query=query_body,
            size=int(size),
            sort=[{"_score": "desc"}, {"timestamp": "desc"}],
        )

        hits = response.get("hits", {}).get("hits", [])
        results = []
        for hit in hits:
            source = hit.get("_source", {})
            results.append(
                {
                    "sequence_number": source.get("sequence_number"),
                    "timestamp": source.get("timestamp"),
                    "host": source.get("host"),
                    "template_id": source.get("template_id"),
                    "label": source.get("label"),
                    "importance_score": source.get("importance_score"),
                    "message": source.get("message"),
                    "correlation_id": (
                        source.get("correlation_id") or source.get("incident_id")
                    ),
                    "_score": hit.get("_score", 0),
                }
            )
        return results

    except Exception as exc:
        logger.warning("Elasticsearch search_logs failed: %s", exc)
        # Reset client so next call gets a fresh connection
        global _ES_CLIENT
        _ES_CLIENT = None
        return []


def get_log_count_by_label(time_range_hours: int = 24) -> dict:
    """Return label → count aggregation for a time window."""
    try:
        client = _get_client()
        if client is None:
            return {}

        response = client.search(
            index=INDEX_NAME,
            size=0,
            query=_time_filter(client, time_range_hours),
            aggs={
                "by_label": {
                    "terms": {"field": "label.keyword", "size": 10}
                }
            },
        )
        buckets = (
            response.get("aggregations", {})
            .get("by_label", {})
            .get("buckets", [])
        )
        return {b["key"]: b["doc_count"] for b in buckets}

    except Exception as exc:
        logger.warning("Elasticsearch get_log_count_by_label failed: %s", exc)
        return {}


def is_elasticsearch_healthy() -> bool:
    """Quick health check — used by the dashboard to show ES status."""
    try:
        client = _get_client()
        if client is None:
            return False
        return client.ping()
    except Exception:
        return False