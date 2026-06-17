"""
parsing/sessionizer.py
======================
Converts a raw syslog file (or directory of syslog files) into
sessionized_logs.parquet.

Pipeline
--------
1. Read raw log lines from the input file (or all .log/.txt files in a directory).
2. Normalize each line via normalizer.normalize_line() →
   {raw_text, timestamp, host, service, log_level, message}.
3. Parse the message through DrainParser to obtain template_id.
4. Group events into sessions: same host + no gap > SESSION_GAP_SECONDS.
5. Derive event_type (= service) and event_action (= template_id with
   service prefix stripped, or first-underscore split as fallback).
6. Compute frequency: count of this template_id within its session.
7. Write data/processed/sessionized_logs.parquet.

Output schema
-------------
sequence_number  int       -- universal join key (1-based, monotonically increasing)
timestamp        datetime
source_type      str       -- always 'switch' for HPE CX logs
service          str       -- normalised subsystem name (OSPF, BGP, SYSTEM, ...)
host             str       -- device hostname
log_level        str       -- CRITICAL | ERROR | WARN | INFO
event_type       str       -- subsystem label (= service)
event_action     str       -- specific action (template_id minus service prefix)
template_id      str       -- Drain template slug
frequency        int       -- count of this template in the same session
event_weight     float     -- severity weight: CRITICAL=1.0, ERROR=0.7, WARN=0.4, INFO=0.1
message          str       -- log message content (severity tokens stripped for Drain)
metadata         str       -- JSON: {"raw_text": "<original line>"}
session_id       str       -- groups related events; not in canonical DB schema
                              but kept here for downstream feature engineering
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List

import pandas as pd

from parsing.normalizer import normalize_line
from parsing.log_parser import DrainParser
from common.config import (
    SESSION_GAP_SECONDS,
    SESSIONIZED_LOGS_PATH,
    SEVERITY_WEIGHTS,
    DEFAULT_SEVERITY_WEIGHT,
    DEFAULT_SOURCE_TYPE,
)
from common.logger import get_logger
from common.utils import save_parquet, validate_schema

logger = get_logger(__name__)

REQUIRED_OUTPUT_COLUMNS = [
    "sequence_number",
    "timestamp",
    "source_type",
    "service",
    "host",
    "log_level",
    "event_type",
    "event_action",
    "template_id",
    "frequency",
    "event_weight",
    "session_id",
    "message",
    "metadata",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _assign_sessions(df: pd.DataFrame) -> pd.DataFrame:
    """Add session_id column by grouping on host + time gap."""
    df = df.sort_values(["host", "timestamp"]).reset_index(drop=True)

    ts_series = pd.to_datetime(df["timestamp"])
    gap_seconds = ts_series.diff().dt.total_seconds().fillna(float("inf"))
    host_changed = df["host"] != df["host"].shift(1)

    session_ids = []
    current_session_id: str | None = None
    _seen_bases: dict[str, int] = {}
    for i in range(len(df)):
        if host_changed.iloc[i] or gap_seconds.iloc[i] > SESSION_GAP_SECONDS:
            session_start_ts = ts_series.iloc[i].to_pydatetime()
            host = df["host"].iloc[i]
            base = f"{host}_{session_start_ts.strftime('%Y%m%dT%H%M%S')}"
            count = _seen_bases.get(base, 0) + 1
            _seen_bases[base] = count
            current_session_id = base if count == 1 else f"{base}_{count}"
        session_ids.append(current_session_id)

    df["session_id"] = session_ids
    return df


def _derive_event_action(service: str, template_id: str) -> str:
    """Return the action portion of template_id with the service prefix removed.

    e.g. service="OSPF", template_id="OSPF_NEIGHBOR_STATE_CHANGE"
         → "NEIGHBOR_STATE_CHANGE"

    Falls back to splitting on the first underscore when the template_id does
    not start with the service name.
    """
    prefix = service + "_"
    if template_id.startswith(prefix):
        return template_id[len(prefix):]
    parts = template_id.split("_", 1)
    return parts[1] if len(parts) > 1 else template_id


def _parse_lines_into_rows(
    file_path: Path,
    parser: DrainParser,
    start_seq_num: int = 1,
) -> tuple[list[dict], int, int]:
    """Feed a single file's lines through the normalizer and DrainParser.

    Returns:
        (rows, next_seq_num, skipped_count)
        rows contains dicts with _cluster (not yet resolved to template_id).
    """
    rows: list[dict] = []
    seq_num = start_seq_num
    skipped = 0

    with open(file_path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            parsed = normalize_line(line)
            if parsed is None:
                skipped += 1
                continue

            cluster = parser.add_log_message_cluster(
                parsed["message"], sequence_number=seq_num
            )

            rows.append({
                "sequence_number": seq_num,
                "timestamp":       parsed["timestamp"],
                "source_type":     DEFAULT_SOURCE_TYPE,
                "service":         parsed["service"],
                "host":            parsed["host"],
                "log_level":       parsed["log_level"],
                "_cluster":        cluster,
                "event_weight":    SEVERITY_WEIGHTS.get(
                    parsed["log_level"], DEFAULT_SEVERITY_WEIGHT
                ),
                "message":         parsed["message"],
                "_raw_text":       parsed["raw_text"],
            })
            seq_num += 1

    return rows, seq_num, skipped


def _rows_to_dataframe(rows: list[dict], parser: DrainParser) -> pd.DataFrame:
    """Resolve Drain templates, assign sessions, compute derived columns."""
    # Pass 2: resolve final collision-safe template slugs now that Drain is stable.
    for row in rows:
        row["template_id"] = parser.resolve_template_id(row.pop("_cluster"))

    df = pd.DataFrame(rows)
    df = _assign_sessions(df)

    df["event_type"] = df["service"]
    df["event_action"] = df.apply(
        lambda r: _derive_event_action(r["service"], r["template_id"]), axis=1
    )

    df["frequency"] = (
        df.groupby(["session_id", "template_id"])["template_id"]
        .transform("count")
        .astype(int)
    )

    df["metadata"] = df["_raw_text"].apply(lambda t: json.dumps({"raw_text": t}))
    df = df.drop(columns=["_raw_text"])
    return df


# ---------------------------------------------------------------------------
# Public API — single file
# ---------------------------------------------------------------------------

def run(
    input_path: str,
    output_path: str = SESSIONIZED_LOGS_PATH,
) -> pd.DataFrame:
    """Parse a raw syslog file and write sessionized_logs.parquet.

    Args:
        input_path:  Path to a raw syslog text file.
        output_path: Destination parquet path (parent dirs created if needed).

    Returns:
        The sessionized DataFrame.

    Raises:
        FileNotFoundError: If input_path does not exist.
        ValueError:        If no parseable log lines are found.
    """
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input log file not found: {input_path}")

    logger.info(f"Reading raw logs from {input_path}")

    parser = DrainParser()
    rows, _, skipped = _parse_lines_into_rows(input_path, parser, start_seq_num=1)

    if not rows:
        raise ValueError(
            f"No parseable log lines found in {input_path}. "
            "Check that the file contains syslog-formatted entries."
        )

    logger.info(f"Parsed {len(rows):,} lines ({skipped} skipped) from {input_path}")

    df = _rows_to_dataframe(rows, parser)

    validate_schema(df, REQUIRED_OUTPUT_COLUMNS)
    save_parquet(df[REQUIRED_OUTPUT_COLUMNS], output_path)

    logger.info(
        f"Wrote {len(df):,} rows → {output_path} "
        f"({df['session_id'].nunique()} sessions, "
        f"{df['template_id'].nunique()} templates)"
    )
    return df


# ---------------------------------------------------------------------------
# Public API — directory of syslog files (Feature #51)
# ---------------------------------------------------------------------------

def run_directory(
    input_dir: str,
    output_path: str = SESSIONIZED_LOGS_PATH,
) -> pd.DataFrame:
    """Parse all .log and .txt files in a directory as one continuous syslog stream.

    Files are processed in sorted filename order so sequence_number is
    deterministic across repeated runs.  A single DrainParser instance is
    shared across all files so templates are built from the full corpus.

    Args:
        input_dir:   Path to a directory containing .log and/or .txt files.
        output_path: Destination parquet path (parent dirs created if needed).

    Returns:
        The combined sessionized DataFrame.

    Raises:
        FileNotFoundError: If input_dir does not exist or is not a directory.
        ValueError:        If no .log/.txt files are found, or none have
                           parseable log lines.
    """
    dir_path = Path(input_dir)
    if not dir_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    if not dir_path.is_dir():
        raise FileNotFoundError(f"Expected a directory, got a file: {input_dir}")

    # Collect and sort files deterministically.
    log_files: List[Path] = sorted(
        [f for f in dir_path.iterdir()
         if f.is_file() and f.suffix.lower() in (".log", ".txt")]
    )

    if not log_files:
        raise ValueError(
            f"No .log or .txt files found in {input_dir}. "
            "Check that the directory contains syslog-formatted log files."
        )

    logger.info(
        f"run_directory: processing {len(log_files)} file(s) from {input_dir}"
    )

    # Single shared DrainParser so templates are learned across all files.
    parser = DrainParser()
    all_rows: list[dict] = []
    total_skipped = 0
    seq_num = 1  # continuous across files

    for log_file in log_files:
        logger.info(f"  Reading: {log_file.name}")
        file_rows, seq_num, skipped = _parse_lines_into_rows(
            log_file, parser, start_seq_num=seq_num
        )
        all_rows.extend(file_rows)
        total_skipped += skipped
        logger.info(
            f"    → {len(file_rows):,} parsed, {skipped} skipped"
        )

    if not all_rows:
        raise ValueError(
            f"No parseable log lines found across all files in {input_dir}. "
            "Check that the files contain syslog-formatted entries."
        )

    logger.info(
        f"run_directory: total {len(all_rows):,} lines parsed "
        f"({total_skipped} skipped) across {len(log_files)} file(s)"
    )

    df = _rows_to_dataframe(all_rows, parser)

    validate_schema(df, REQUIRED_OUTPUT_COLUMNS)
    save_parquet(df[REQUIRED_OUTPUT_COLUMNS], output_path)

    logger.info(
        f"Wrote {len(df):,} rows → {output_path} "
        f"({df['session_id'].nunique()} sessions, "
        f"{df['template_id'].nunique()} templates)"
    )
    return df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Sessionize a raw syslog file or directory.")
    ap.add_argument(
        "input",
        nargs="?",
        default="data/raw/sample.log",
        help="Path to raw syslog file or directory (default: data/raw/sample.log)",
    )
    ap.add_argument(
        "--output",
        default=SESSIONIZED_LOGS_PATH,
        help=f"Output parquet path (default: {SESSIONIZED_LOGS_PATH})",
    )
    args = ap.parse_args()

    p = Path(args.input)
    if p.is_dir():
        df = run_directory(args.input, args.output)
    else:
        df = run(args.input, args.output)

    print(f"Sessions : {df['session_id'].nunique()}")
    print(f"Templates: {df['template_id'].nunique()}")
    print(f"Rows     : {len(df):,}")
    print(f"\nSchema:\n{df.dtypes}")
    print(f"\nSample:\n{df.head(3).to_string()}")