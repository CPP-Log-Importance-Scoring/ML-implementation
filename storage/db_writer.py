import json
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

from common.config import DB_URL

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"
SYSTEM_COLUMNS = {"created_at", "updated_at"}


def get_connection(conn: Optional[psycopg2.extensions.connection] = None):
    return conn or psycopg2.connect(DB_URL)


def apply_schema(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.commit()


def _table_columns(conn, table_name: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [r[0] for r in cur.fetchall()]


def _normalize_df(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    normalized = df.copy()
    for c in columns:
        if c not in normalized.columns:
            normalized[c] = None

    for c in normalized.columns:
        if normalized[c].dtype == "object":
            normalized[c] = normalized[c].map(
                lambda x: Json(x) if isinstance(x, (dict, list)) else x
            )

    ordered = [c for c in columns if c in normalized.columns]
    return normalized[ordered]


def _upsert_key_for_table(table_name: str) -> str:
    return "incident_id" if table_name == "incidents" else "log_id"


def write_dataframe(
    df: pd.DataFrame,
    table_name: str,
    conn: Optional[psycopg2.extensions.connection] = None,
) -> int:
    if df is None or df.empty:
        return 0

    owned_conn = conn is None
    conn = get_connection(conn)
    try:
        table_cols = [c for c in _table_columns(conn, table_name) if c not in SYSTEM_COLUMNS]
        if not table_cols:
            raise ValueError(f"Table `{table_name}` not found or has no writable columns")

        use_df = _normalize_df(df, table_cols)
        key_col = _upsert_key_for_table(table_name)
        if key_col not in use_df.columns:
            raise ValueError(f"Missing required key column `{key_col}` for table `{table_name}`")

        insert_cols = [c for c in table_cols if c in use_df.columns]
        update_cols = [c for c in insert_cols if c != key_col]

        update_clause = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in update_cols
        )

        query = sql.SQL(
            """
            INSERT INTO {table} ({columns}) VALUES %s
            ON CONFLICT ({key_col})
            DO UPDATE SET {updates}, updated_at = NOW()
            """
        ).format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(", ").join(sql.Identifier(c) for c in insert_cols),
            key_col=sql.Identifier(key_col),
            updates=update_clause,
        )

        rows = [tuple(row[c] for c in insert_cols) for _, row in use_df.iterrows()]
        with conn.cursor() as cur:
            execute_values(cur, query.as_string(conn), rows, page_size=1000)
        conn.commit()
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        if owned_conn:
            conn.close()


def write_logs(df: pd.DataFrame, conn=None) -> int:
    return write_dataframe(df, "logs", conn)


def write_features(df: pd.DataFrame, conn=None) -> int:
    return write_dataframe(df, "features", conn)


def write_anomalies(df: pd.DataFrame, conn=None) -> int:
    return write_dataframe(df, "anomalies", conn)


def write_scores(df: pd.DataFrame, conn=None) -> int:
    return write_dataframe(df, "scores", conn)


def write_incidents(df: pd.DataFrame, conn=None) -> int:
    return write_dataframe(df, "incidents", conn)


def _synthetic_data(n: int = 200) -> dict[str, pd.DataFrame]:
    timestamps = pd.date_range("2026-05-01", periods=n, freq="min")
    log_ids = [f"log_{i:06d}" for i in range(n)]
    labels = ["ignore", "low", "medium", "critical"]

    logs = pd.DataFrame(
        {
            "log_id": log_ids,
            "sequence_number": range(n),
            "timestamp": timestamps,
            "source_type": "synthetic",
            "service": "pipeline",
            "host": "localhost",
            "log_level": "INFO",
            "event_type": "heartbeat",
            "event_action": "tick",
            "template_id": "tmpl_1",
            "message": [f"synthetic message {i}" for i in range(n)],
            "raw_text": [f"raw synthetic log {i}" for i in range(n)],
            "metadata": [
                {
                    "incident_id": f"inc_{i // 20:03d}",
                    "label": labels[i % 4],
                }
                for i in range(n)
            ],
            "session_id": [f"s_{i // 10:03d}" for i in range(n)],
        }
    )

    features = pd.DataFrame(
        {
            "log_id": log_ids,
            "timestamp": timestamps,
            "label": [labels[i % 4] for i in range(n)],
            "incident_id": [f"inc_{i // 20:03d}" for i in range(n)],
            "frequency": 1,
            "event_weight": 0.3,
            "frequency_score": 0.2,
            "severity_weight": 0.4,
            "counter_proximity": 0.1,
            "feature_payload": [{"source": "synthetic"}] * n,
            "in_sequence": [i % 2 == 0 for i in range(n)],
        }
    )

    anomalies = pd.DataFrame(
        {
            "log_id": log_ids,
            "incident_id": [f"inc_{i // 20:03d}" for i in range(n)],
            "timestamp": timestamps,
            "label": [labels[i % 4] for i in range(n)],
            "isolation_score": 0.5,
            "zscore": 0.4,
            "anomaly_score": [0.9 if i % 13 == 0 else 0.2 for i in range(n)],
            "is_anomaly": [i % 13 == 0 for i in range(n)],
            "in_sequence": [i % 2 == 0 for i in range(n)],
        }
    )

    scores = pd.DataFrame(
        {
            "log_id": log_ids,
            "importance_score": [min(1.0, (i % 100) / 100) for i in range(n)],
            "final_score": [min(1.0, (i % 100) / 100) for i in range(n)],
            "label": [labels[i % 4] for i in range(n)],
            "correlation_id": [f"corr_{i // 10:03d}" for i in range(n)],
            "incident_id": [f"inc_{i // 20:03d}" for i in range(n)],
            "is_root_cause": [i % 25 == 0 for i in range(n)],
            "root_cause_confidence": [0.85 if i % 25 == 0 else 0.15 for i in range(n)],
            "in_sequence": [i % 2 == 0 for i in range(n)],
            "timestamp": timestamps,
        }
    )

    incidents = pd.DataFrame(
        {
            "incident_id": [f"inc_{i:03d}" for i in range(max(1, n // 20))],
            "start_time": [timestamps[i * 20] for i in range(max(1, n // 20))],
            "end_time": [timestamps[min(n - 1, i * 20 + 19)] for i in range(max(1, n // 20))],
            "root_cause_log_id": [f"log_{i * 20:06d}" for i in range(max(1, n // 20))],
            "severity": "medium",
            "label": "medium",
            "root_cause_confidence": 0.8,
            "log_count": 20,
            "status": "open",
        }
    )

    return {
        "logs": logs,
        "features": features,
        "anomalies": anomalies,
        "scores": scores,
        "incidents": incidents,
    }


def seed_synthetic_data(conn=None, n: int = 200) -> dict[str, int]:
    owned_conn = conn is None
    conn = get_connection(conn)
    try:
        apply_schema(conn)
        data = _synthetic_data(n)
        out = {}
        for table in ["logs", "features", "anomalies", "scores", "incidents"]:
            out[table] = write_dataframe(data[table], table, conn)
        return out
    finally:
        if owned_conn:
            conn.close()


if __name__ == "__main__":
    counts = seed_synthetic_data(n=250)
    print(json.dumps({"seeded_rows": counts}, indent=2))
