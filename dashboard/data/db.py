"""
dashboard/data/db.py
====================
Postgres query helpers for the HPE CX Incident Intelligence Dashboard.

All functions:
  - Use a connection pool (psycopg2.pool.SimpleConnectionPool)
  - Use parameterised queries only — NO f-string SQL
  - Return empty list / empty DataFrame / None on error — never raise to caller
  - Accept both time_range_hours (legacy) and start_time/end_time (new)
"""

from __future__ import annotations

import pandas as pd
from datetime import datetime, timedelta
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, execute_values

from common.config import DB_URL
from common.logger import get_logger

logger = get_logger(__name__)

_POOL = None


# ---------------------------------------------------------------------------
# Pool management
# ---------------------------------------------------------------------------

def _init_pool() -> pool.SimpleConnectionPool:
    global _POOL
    if _POOL is None:
        _POOL = pool.SimpleConnectionPool(1, 10, dsn=DB_URL)
    return _POOL


def _get_connection():
    try:
        return _init_pool().getconn()
    except Exception as exc:
        logger.warning("Unable to acquire DB connection: %s", exc)
        return None


def is_db_healthy() -> bool:
    """Return True when the Postgres connection pool can execute a simple query."""
    conn = _get_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception as exc:
        logger.warning("DB health check failed: %s", exc)
        return False
    finally:
        _release_connection(conn)


def _release_connection(conn) -> None:
    if conn is None:
        return
    try:
        _init_pool().putconn(conn)
    except Exception as exc:
        logger.warning("Unable to release DB connection: %s", exc)


def _query_dataframe(query: str, params: tuple = ()) -> pd.DataFrame:
    conn = _get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            return pd.DataFrame(rows)
    except Exception as exc:
        logger.warning("DB query failed: %s", exc)
        return pd.DataFrame()
    finally:
        _release_connection(conn)


# ---------------------------------------------------------------------------
# Time window helpers
# ---------------------------------------------------------------------------

def _resolve_time_window(
    time_range_hours: int | None,
    start_time: datetime | None,
    end_time: datetime | None,
) -> tuple[datetime, datetime]:
    """Return (start_dt, end_dt) from whichever args were provided."""
    now = datetime.utcnow()
    if start_time is not None and end_time is not None:
        return start_time, end_time
    hours = time_range_hours if time_range_hours is not None else 24
    return now - timedelta(hours=hours), now


# ---------------------------------------------------------------------------
# Public query functions
# ---------------------------------------------------------------------------

def get_incidents(
    host: str | None = None,
    severity: str | list[str] | None = None,
    time_range_hours: int | None = 720,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    cross_system_only: bool = False,
    escalated_only: bool = False,
) -> list[dict]:
    """
    Returns incidents as a list of dicts:
        correlation_id, host, start_time, end_time,
        log_count, label, is_cross_system, duration,
        final_score, root_cause_confidence,
        is_escalated, escalation_reason, n_critical_rows, n_high_severity_rows

    Escalation comes from incident_history via a fail-open LEFT JOIN: an incident
    with no history row (or a DB predating the column) defaults to escalated=TRUE,
    so nothing is ever wrongly hidden. escalated_only=True hides clean-day medium
    noise (incidents carrying no critical-label or high-severity-density signal).
    """
    start_dt, end_dt = _resolve_time_window(time_range_hours, start_time, end_time)

    query_parts = [
        "SELECT",
        "  i.incident_id AS correlation_id,",
        "  CASE WHEN COUNT(DISTINCT l.host) > 1 THEN 'MULTI' ELSE MIN(l.host) END AS host,",
        "  i.start_time AS start_time,",
        "  i.end_time AS end_time,",
        "  i.log_count AS log_count,",
        "  i.label AS label,",
        "  i.root_cause_confidence AS root_cause_confidence,",
        "  MAX(s.final_score) AS final_score,",
        "  COUNT(DISTINCT l.host) > 1 AS is_cross_system,",
        "  EXTRACT(EPOCH FROM (i.end_time - i.start_time))::int AS duration,",
        "  BOOL_OR(COALESCE(ih.is_escalated, TRUE)) AS is_escalated,",
        "  MAX(ih.escalation_reason) AS escalation_reason,",
        "  MAX(ih.n_critical_rows) AS n_critical_rows,",
        "  MAX(ih.n_high_severity_rows) AS n_high_severity_rows",
        "FROM incidents i",
        "JOIN scores s ON s.correlation_id = i.incident_id",
        "JOIN logs l ON l.sequence_number = s.sequence_number AND l.run_id = s.run_id",
        "LEFT JOIN incident_history ih ON ih.incident_id = i.incident_id",
        "WHERE i.start_time >= %s",
        "  AND i.start_time <= %s",
    ]
    params: list = [start_dt, end_dt]

    if host is not None:
        query_parts.append("""  AND i.incident_id IN (
            SELECT DISTINCT s2.correlation_id
            FROM scores s2
            JOIN logs l2 ON l2.sequence_number = s2.sequence_number AND l2.run_id = s2.run_id
            WHERE l2.host = %s
        )""")
        params.append(host)

    # severity can be a single string or a list
    if severity is not None:
        if isinstance(severity, str):
            severity = [severity]
        placeholders = ", ".join(["%s"] * len(severity))
        query_parts.append(f"  AND i.label IN ({placeholders})")
        params.extend(severity)

    query_parts.append("GROUP BY i.incident_id, i.start_time, i.end_time, i.log_count, i.label, i.root_cause_confidence")

    having_clauses = []
    if cross_system_only:
        having_clauses.append("COUNT(DISTINCT l.host) > 1")
    if escalated_only:
        having_clauses.append("BOOL_OR(COALESCE(ih.is_escalated, TRUE)) = TRUE")
    if having_clauses:
        query_parts.append("HAVING " + " AND ".join(having_clauses))

    query_parts.append("ORDER BY start_time DESC")
    query_parts.append("LIMIT 200")

    sql = "\n".join(query_parts)
    df = _query_dataframe(sql, tuple(params))
    return df.to_dict(orient="records") if not df.empty else []


def get_incident_logs(correlation_id: str) -> pd.DataFrame:
    """All logs in a given incident with scores + feature columns."""
    query = """
        SELECT
            l.sequence_number AS log_id,
            l.sequence_number,
            l.timestamp,
            l.host,
            l.template_id,
            l.message,
            l.log_level,
            l.service,
            s.correlation_id AS incident_id,
            s.final_score AS importance_score,
            s.final_score,
            s.correlation_id,
            s.is_root_cause,
            s.root_cause_confidence,
            s.label,
            f.frequency_score,
            f.event_weight,
            f.event_weight AS severity_weight,
            f.counter_proximity,
            FALSE AS feature_in_sequence,
            NULL AS feature_payload
        FROM logs l
        JOIN scores s ON s.sequence_number = l.sequence_number AND s.run_id = l.run_id
        LEFT JOIN features f ON f.sequence_number = l.sequence_number AND f.run_id = l.run_id
        WHERE s.correlation_id = %s
        ORDER BY l.timestamp ASC
    """
    return _query_dataframe(query, (correlation_id,))


def get_root_causes(correlation_id: str) -> pd.DataFrame:
    """Root cause candidates for a given incident."""
    query = """
        SELECT
            s.correlation_id AS incident_id,
            'log_' || l.sequence_number AS root_cause_log_id,
            s.root_cause_confidence AS confidence_score,
            TRUE AS in_graph,
            l.template_id,
            l.timestamp,
            l.message,
            l.host
        FROM scores s
        JOIN logs l ON l.sequence_number = s.sequence_number AND l.run_id = s.run_id
        WHERE s.correlation_id = %s
          AND s.is_root_cause = TRUE
        ORDER BY s.root_cause_confidence DESC, s.final_score DESC
        LIMIT 10
    """
    return _query_dataframe(query, (correlation_id,))


def get_host_stats(
    time_range_hours: int | None = 24,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> pd.DataFrame:
    """Per-host incident counts, critical counts, anomaly rate, last incident."""
    start_dt, end_dt = _resolve_time_window(time_range_hours, start_time, end_time)

    query = """
        SELECT
            l.host,
            COUNT(DISTINCT s.correlation_id) AS incident_count,
            SUM(CASE WHEN s.label = 'critical' THEN 1 ELSE 0 END) AS critical_count,
            CASE
                WHEN COUNT(l.sequence_number) = 0 THEN 0.0
                ELSE SUM(CASE WHEN a.is_anomaly = TRUE THEN 1 ELSE 0 END)::double precision
                     / COUNT(l.sequence_number)
            END AS anomaly_rate,
            MAX(l.timestamp) AS last_incident_at
        FROM logs l
        LEFT JOIN scores s ON s.sequence_number = l.sequence_number AND s.run_id = l.run_id
        LEFT JOIN anomalies a ON a.sequence_number = l.sequence_number AND a.run_id = l.run_id
        WHERE l.timestamp >= %s
          AND l.timestamp <= %s
        GROUP BY l.host
        ORDER BY incident_count DESC, critical_count DESC
    """
    return _query_dataframe(query, (start_dt, end_dt))


def get_host_list() -> list[str]:
    """Return all distinct host names in the logs table."""
    df = _query_dataframe("SELECT DISTINCT host FROM logs ORDER BY host")
    if df.empty or "host" not in df.columns:
        return []
    return df["host"].dropna().tolist()

def get_anomaly_count() -> int:
    """Return actual anomaly count."""
    query = """
        SELECT COUNT(*) AS count
        FROM anomalies
        WHERE is_anomaly = TRUE
    """

    df = _query_dataframe(query)

    if df.empty:
        return 0

    return int(df.iloc[0]["count"])


def get_incident_count_by_hour(
    time_range_hours: int = 24,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> pd.DataFrame:
    """Returns incident counts grouped by hour — for the sparkline on the feed page."""
    start_dt, end_dt = _resolve_time_window(time_range_hours, start_time, end_time)
    query = """
        SELECT
            DATE_TRUNC('hour', l.timestamp) AS hour,
            COUNT(DISTINCT s.correlation_id) AS incident_count,
            SUM(CASE WHEN s.label = 'critical' THEN 1 ELSE 0 END) AS critical_count
        FROM logs l
        JOIN scores s ON s.sequence_number = l.sequence_number AND s.run_id = l.run_id
        WHERE l.timestamp >= %s AND l.timestamp <= %s
          AND s.correlation_id IS NOT NULL
        GROUP BY DATE_TRUNC('hour', l.timestamp)
        ORDER BY hour ASC
    """
    return _query_dataframe(query, (start_dt, end_dt))


# ---------------------------------------------------------------------------
# Summary (LLM cache)
# ---------------------------------------------------------------------------

def get_summary(correlation_id: str) -> str | None:
    """Return cached summary text, or None if not yet generated."""
    conn = _get_connection()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT summary_text FROM summaries WHERE correlation_id = %s",
                (correlation_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as exc:
        logger.warning("Failed to fetch summary: %s", exc)
        return None
    finally:
        _release_connection(conn)


def write_summary(correlation_id: str, summary_text: str) -> bool:
    """Upsert a single summary — used by the dashboard Regenerate button only."""
    conn = _get_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO summaries (correlation_id, summary_text)
                VALUES (%s, %s)
                ON CONFLICT (correlation_id)
                DO UPDATE SET summary_text = EXCLUDED.summary_text,
                              updated_at   = NOW()
                """,
                (correlation_id, summary_text),
            )
        conn.commit()
        return True
    except Exception as exc:
        conn.rollback()
        logger.warning("Failed to write summary: %s", exc)
        return False
    finally:
        _release_connection(conn)


def write_summaries_batch(summaries: list[dict]) -> int:
    """
    Bulk upsert a list of {"correlation_id": ..., "summary_text": ...} dicts.
    Single transaction. Called by pipeline.py batch generator — never the dashboard.
    """
    if not summaries:
        return 0
    conn = _get_connection()
    if conn is None:
        return 0

    rows = [
        (item["correlation_id"], item["summary_text"])
        for item in summaries
        if item.get("correlation_id") and item.get("summary_text") is not None
    ]
    if not rows:
        return 0

    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO summaries (correlation_id, summary_text)
                VALUES %s
                ON CONFLICT (correlation_id)
                DO UPDATE SET summary_text = EXCLUDED.summary_text,
                              updated_at   = NOW()
                """,
                rows,
            )
        conn.commit()
        return len(rows)
    except Exception as exc:
        conn.rollback()
        logger.warning("Failed to write summaries batch: %s", exc)
        return 0
    finally:
        _release_connection(conn)