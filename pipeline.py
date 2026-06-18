"""
pipeline.py
===========
End-to-end pipeline orchestrator for the HPE CX log analysis system.

Execution order
---------------
  1. parsing       parsing/sessionizer.py        -> data/processed/sessionized_logs.parquet
  2. features      features/feature_pipeline.py  -> data/processed/features_df.parquet
  3. anomaly       ml/anomaly_detector.py        -> data/processed/anomaly_df.parquet
  4. correlation   correlation/run_correlation.py -> data/processed/graph_scores_df.parquet
  5. scoring       scoring/importance_scorer.py  -> data/processed/scored_logs_df.parquet
  5.5. cross_run  correlation/cross_run.py       -> data/processed/incident_history.parquet
  5.9. evaluate   evaluation/oracle_report.py    -> evaluation/results/oracle_report.txt
                  (skipped unless scenario_labels.parquet exists; never fatal)
  6. storage       storage/db_writer.py          -> Postgres (skipped in --dry-run)

Usage
-----
  # Full run (writes to Postgres)
  python pipeline.py

  # Dry run — all steps except Postgres write
  python pipeline.py --dry-run

  # Restart from a specific step (reads existing parquets for earlier steps)
  python pipeline.py --from-step anomaly

  # Dry run starting from scoring
  python pipeline.py --dry-run --from-step scoring

  # Use a specific raw log file as input to the parsing step
  python pipeline.py --log-file data/raw/cx_switches.log

  # Upload a directory of syslog files (NOT synthetic format)
  python pipeline.py --log-file data/raw/uploads/20260616_153001_8d4c2a --input-mode syslog

Step names (for --from-step)
-----------------------------
  parsing | features | anomaly | correlation | scoring | cross_run | evaluate | storage
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Literal, Optional

from common.logger import get_logger
import common.config as cfg

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Step ordering
# ---------------------------------------------------------------------------

STEPS = ["parsing", "features", "anomaly", "correlation", "scoring", "cross_run", "evaluate", "storage"]

# ---------------------------------------------------------------------------
# Output paths are centralized in common/config.py
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Step implementations
# ---------------------------------------------------------------------------

def _step_parsing(log_file: str, input_mode: str = "auto") -> int:
    """Run the parsing / sessionization step.

    Reads a raw syslog file (or directory of syslog files) and writes
    sessionized_logs.parquet.

    Args:
        log_file:   Path to a raw syslog file or directory.
        input_mode: One of "auto" | "syslog" | "synthetic".
                    - "auto"      : existing behaviour (dir→synthetic, file→sessionizer).
                    - "syslog"    : always use the real syslog sessionizer.
                                    Directories are handled via sessionizer.run_directory().
                    - "synthetic" : always use the synthetic dataset loader.
    """
    p = Path(log_file)

    # ------------------------------------------------------------------ #
    # Resolve effective mode
    # ------------------------------------------------------------------ #
    if input_mode == "auto":
        if p.is_dir():
            effective = "synthetic"
        else:
            effective = "syslog"
    else:
        effective = input_mode  # "syslog" or "synthetic"

    # ------------------------------------------------------------------ #
    # Dispatch
    # ------------------------------------------------------------------ #
    if effective == "synthetic":
        logger.info(f"Parsing synthetic dataset directory: {log_file}")
        from parsing.synthetic_dataset_loader import run as load_synthetic
        df = load_synthetic(log_file, output_path=cfg.SESSIONIZED_LOGS_PATH)
    elif effective == "syslog":
        if p.is_dir():
            logger.info(f"Parsing syslog directory (run_directory): {log_file}")
            from parsing.sessionizer import run_directory
            df = run_directory(log_file, output_path=cfg.SESSIONIZED_LOGS_PATH)
        elif p.exists():
            logger.info(f"Parsing raw log file: {log_file}")
            from parsing.sessionizer import run as sessionize
            df = sessionize(log_file, output_path=cfg.SESSIONIZED_LOGS_PATH)
        else:
            logger.warning(
                f"Log file not found: {log_file}. "
                "Generating synthetic data via real parsing pipeline."
            )
            from scripts.generate_real_logs import generate_dataset
            from common.utils import save_parquet

            # generate_dataset() runs the real parser and returns the canonical DataFrame.
            # Re-save under the pipeline's path in case config paths differ.
            df = generate_dataset()
            Path(cfg.SESSIONIZED_LOGS_PATH).parent.mkdir(parents=True, exist_ok=True)
            save_parquet(df, cfg.SESSIONIZED_LOGS_PATH)
    else:
        raise ValueError(f"Unknown input_mode: {input_mode!r}. Use 'auto', 'syslog', or 'synthetic'.")

    return len(df)


def _step_features() -> int:
    """Run the feature engineering step."""
    from features.feature_pipeline import run_pipeline
    df = run_pipeline(cfg.SESSIONIZED_LOGS_PATH)
    return len(df)


def _step_anomaly() -> int:
    """Run the ML anomaly detection step."""
    from pathlib import Path as _Path
    from ml.anomaly_detector import run
    df = run(
        features_path=Path(cfg.FEATURES_OUTPUT_PATH),
        output_path=Path(cfg.ANOMALY_PATH),
    )
    return len(df)


def _step_correlation() -> int:
    """Run the graph correlation step.

    Always forces a graph rebuild to prevent stale cached graphs (built from
    different template data) from producing all-UNCAPPED centrality scores.
    """
    import os
    import common.config as cfg

    graph_cache = cfg.GRAPH_PICKLE_PATH
    if os.path.exists(graph_cache):
        os.remove(graph_cache)
        logger.info(f"Removed stale graph cache: {graph_cache}")

    from correlation.run_correlation import run
    run()

    import pandas as pd
    df = pd.read_parquet(cfg.GRAPH_SCORES_PATH)
    return len(df)


def _step_scoring() -> int:
    """Run the importance scoring step."""
    from scoring.importance_scorer import run
    df = run()
    return len(df)


def _step_cross_run(dry_run: bool) -> int:
    """Run the cross-run incident correlation step (P5.5)."""
    from correlation.cross_run import run as cross_run
    scored_df, _ = cross_run(dry_run=dry_run)
    return len(scored_df)


def _step_evaluate() -> int:
    """Evaluate pipeline output against the Section-7 oracle (P5.9)."""
    if not Path(cfg.SCENARIO_LABELS_PATH).exists():
        logger.info(
            "No scenario labels at %s — skipping oracle evaluation.",
            cfg.SCENARIO_LABELS_PATH,
        )
        return 0

    try:
        from evaluation.oracle_report import run_oracle_report
        metrics = run_oracle_report()
        return metrics.get("total_logs", 0)
    except Exception as exc:
        logger.warning("Oracle evaluation failed (non-fatal): %s", exc)
        return 0


def _compute_run_id() -> str:
    """Batch run id = YYYYMMDD of the earliest event in this run.

    Deterministic from the data (idempotent on re-run) and distinct across
    non-overlapping batches, matching correlation/cross_run.py's run_date_str.
    """
    import pandas as pd
    from datetime import datetime, timezone

    try:
        df = pd.read_parquet(cfg.SESSIONIZED_LOGS_PATH, columns=["timestamp"])
        ts = pd.to_datetime(df["timestamp"], errors="coerce").dropna()
        if len(ts):
            return ts.min().strftime("%Y%m%d")
    except Exception:
        pass
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _step_storage(dry_run: bool) -> int:
    """Write all parquets to Postgres (skipped in dry-run mode)."""
    if dry_run:
        logger.info("--dry-run: skipping Postgres write.")
        return 0

    import pandas as pd
    from storage.db_writer import (
        apply_schema,
        get_connection,
        write_anomalies,
        write_features,
        write_incidents,
        write_logs,
        write_scores,
    )

    conn = get_connection()
    try:
        apply_schema(conn)

        counts = {}

        # Run identifier (YYYYMMDD of the batch's earliest event) — stamped on
        # every per-batch table so (run_id, sequence_number) is globally unique
        # and successive upload batches ACCUMULATE instead of overwriting.
        run_id = _compute_run_id()
        logger.info("Storage run_id=%s", run_id)

        def _globalize_id(cid):
            """Local INC-NNNN -> globally-unique INC-<run_id>-NNNN."""
            if pd.isna(cid):
                return cid
            return f"INC-{run_id}-{str(cid).replace('INC-', '')}"

        if Path(cfg.SESSIONIZED_LOGS_PATH).exists():
            logs_df = pd.read_parquet(cfg.SESSIONIZED_LOGS_PATH)
            logs_df["run_id"] = run_id
            counts["logs"] = write_logs(logs_df, conn)

        if Path(cfg.FEATURES_OUTPUT_PATH).exists():
            feat_df = pd.read_parquet(cfg.FEATURES_OUTPUT_PATH)
            feat_df["run_id"] = run_id
            counts["features"] = write_features(feat_df, conn)

        if Path(cfg.ANOMALY_PATH).exists():
            anom_df = pd.read_parquet(cfg.ANOMALY_PATH)
            anom_df["run_id"] = run_id
            counts["anomalies"] = write_anomalies(anom_df, conn)

        if Path(cfg.SCORED_LOGS_PATH).exists():
            scored_df = pd.read_parquet(cfg.SCORED_LOGS_PATH)
            scored_df["run_id"] = run_id
            # Write scores with a globalized correlation_id so it matches
            # incidents.incident_id; keep the local scored_df for the incident
            # aggregation below (root-cause merge still joins on local ids).
            scored_write = scored_df.copy()
            scored_write["correlation_id"] = scored_write["correlation_id"].map(_globalize_id)
            counts["scores"] = write_scores(scored_write, conn)

            if Path("data/processed/root_causes_df.parquet").exists():
                from common.utils import worst_label

                rc_df = pd.read_parquet("data/processed/root_causes_df.parquet")
                logs_df = pd.read_parquet(cfg.SESSIONIZED_LOGS_PATH)
                scores_with_ts = scored_df.merge(
                    logs_df[["sequence_number", "timestamp"]],
                    on="sequence_number",
                    how="left",
                )
                incidents_df = (
                    scores_with_ts.groupby("correlation_id")
                    .agg(
                        start_time=("timestamp", "min"),
                        end_time=("timestamp", "max"),
                        log_count=("sequence_number", "count"),
                        label=("label", worst_label),
                    )
                    .reset_index()
                )
                incidents_df["severity"] = incidents_df["label"]
                incidents_df["status"] = "open"

                rc_best = rc_df.sort_values("confidence_score", ascending=False).drop_duplicates("incident_id")
                incidents_df = incidents_df.merge(
                    rc_best,
                    left_on="correlation_id",
                    right_on="incident_id",
                    how="left",
                )
                incidents_df = incidents_df.rename(columns={"confidence_score": "root_cause_confidence"})

                def _normalize_root_cause_log_id(value):
                    if pd.isna(value):
                        return None
                    if isinstance(value, str):
                        text = value.strip()
                        if text.startswith("log_"):
                            return text
                        return f"log_{int(float(text)):06d}"
                    return f"log_{int(value):06d}"

                incidents_df["root_cause_log_id"] = incidents_df["root_cause_log_id"].apply(_normalize_root_cause_log_id)
                # Globalize the incident id (was the local INC-NNNN from the
                # correlation_id groupby) so incidents accumulate across batches
                # and match scores.correlation_id written above.
                incidents_df["run_id"] = run_id
                incidents_df["incident_id"] = incidents_df["correlation_id"].map(_globalize_id)
                counts["incidents"] = write_incidents(incidents_df, conn)

        if not dry_run and Path(cfg.SCORED_LOGS_PATH).exists():
            try:
                import pandas as _pd
                from pathlib import Path as _Path
                from dashboard.llm_summary import (
                    generate_all_summaries as _gen_summaries,
                )

                _scored_df = _pd.read_parquet(cfg.SCORED_LOGS_PATH)

                _rc_df = _pd.DataFrame()
                _rc_path = "data/processed/root_causes_df.parquet"
                if _Path(_rc_path).exists():
                    _rc_df = _pd.read_parquet(_rc_path)

                _gen_summaries(_scored_df, _rc_df, batch_size=20)
                logger.info("LLM summaries generated and cached.")

            except Exception as _exc:
                logger.warning(
                    "LLM summary generation failed (non-fatal): %s",
                    _exc,
                )

        conn.commit()
        logger.info(f"Postgres write counts: {counts}")
        return sum(counts.values())

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def _run_step(
    step: str,
    dry_run: bool,
    log_file: str,
    input_mode: str = "auto",
) -> int:
    """Dispatch to the correct step function and return row count."""
    dispatch = {
        "parsing":     lambda: _step_parsing(log_file, input_mode=input_mode),
        "features":    _step_features,
        "anomaly":     _step_anomaly,
        "correlation": _step_correlation,
        "scoring":     _step_scoring,
        "cross_run":   lambda: _step_cross_run(dry_run),
        "evaluate":    _step_evaluate,
        "storage":     lambda: _step_storage(dry_run),
    }
    return dispatch[step]()


def run_pipeline(
    dry_run: bool = False,
    from_step: Optional[str] = None,
    log_file: str = "data/raw/sample.log",
    input_mode: str = "auto",
) -> None:
    """Run the full pipeline from from_step onwards.

    Args:
        dry_run:    Skip the Postgres write step.
        from_step:  Name of the step to start from (skips earlier steps,
                    reading their parquet outputs from disk instead).
        log_file:   Path to the raw log file (or directory) for the parsing step.
        input_mode: "auto" | "syslog" | "synthetic".
                    Controls how the parsing step interprets a directory input.
                    "auto" preserves the original behaviour (back-compat default).
    """
    if from_step and from_step not in STEPS:
        logger.error(
            f"Unknown step '{from_step}'. Valid steps: {', '.join(STEPS)}"
        )
        sys.exit(1)

    start_idx = STEPS.index(from_step) if from_step else 0
    active_steps = STEPS[start_idx:]

    mode_tag = "[DRY-RUN] " if dry_run else ""
    logger.info(f"{mode_tag}Pipeline starting — steps: {', '.join(active_steps)}")
    logger.info(f"input_mode={input_mode!r}  log_file={log_file!r}")

    total_start = time.perf_counter()

    for step in active_steps:
        t0 = time.perf_counter()
        logger.info(f"{'='*50}")
        logger.info(f"STEP: {step.upper()}")

        try:
            row_count = _run_step(
                step,
                dry_run=dry_run,
                log_file=log_file,
                input_mode=input_mode,
            )
            elapsed = time.perf_counter() - t0
            logger.info(
                f"DONE: {step} — {row_count:,} rows — {elapsed:.2f}s"
            )

            if row_count == 0 and step not in ("evaluate", "storage"):
                logger.error(f"FAILED at step '{step}': Produced 0 rows. Stopping pipeline.")
                sys.exit(1)

        except FileNotFoundError as exc:
            logger.error(
                f"FAILED at step '{step}': {exc}\n"
                f"Hint: use --from-step to restart from an earlier step, "
                f"or ensure the upstream parquet exists."
            )
            sys.exit(1)

        except Exception as exc:
            logger.error(f"FAILED at step '{step}': {type(exc).__name__}: {exc}")
            raise

    total_elapsed = time.perf_counter() - total_start
    logger.info("=" * 50)
    logger.info(
        f"{mode_tag}Pipeline complete — {len(active_steps)} steps in "
        f"{total_elapsed:.2f}s"
    )

    if dry_run:
        logger.info("Dry run finished. Postgres write was skipped.")

    _print_output_summary()


def _print_output_summary() -> None:
    """Log a summary of output files produced."""
    outputs = [
        ("sessionized_logs",   cfg.SESSIONIZED_LOGS_PATH),
        ("features_df",        cfg.FEATURES_OUTPUT_PATH),
        ("anomaly_df",         cfg.ANOMALY_PATH),
        ("graph_scores_df",    cfg.GRAPH_SCORES_PATH),
        ("scored_logs_df",     cfg.SCORED_LOGS_PATH),
        ("incident_history",   cfg.INCIDENT_HISTORY_PATH),
    ]
    logger.info("Output files:")
    for name, path in outputs:
        p = Path(path)
        if p.exists():
            size_kb = p.stat().st_size / 1024
            logger.info(f"  {name:<22} {path}  ({size_kb:.1f} KB)")
        else:
            logger.info(f"  {name:<22} {path}  (not produced)")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Run the HPE CX log analysis pipeline end-to-end.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Run all steps but skip the Postgres write (step 6).",
    )
    ap.add_argument(
        "--from-step",
        metavar="STEP",
        choices=STEPS,
        default=None,
        help=(
            f"Start from this step, reading upstream parquets from disk. "
            f"Choices: {', '.join(STEPS)}"
        ),
    )
    ap.add_argument(
        "--log-file",
        metavar="PATH",
        default="data/raw/sample.log",
        help=(
            "Path to the raw syslog file (or directory) for the parsing step. "
            "If the file does not exist, synthetic data is generated. "
            "(default: data/raw/sample.log)"
        ),
    )
    ap.add_argument(
        "--input-mode",
        metavar="MODE",
        choices=["auto", "syslog", "synthetic"],
        default="auto",
        help=(
            "How to interpret a directory passed to --log-file. "
            "'auto' keeps the original behaviour (dir→synthetic, file→sessionizer). "
            "'syslog' always uses the real syslog sessionizer (use for uploaded log dirs). "
            "'synthetic' always uses the synthetic dataset loader. "
            "(default: auto)"
        ),
    )
    args = ap.parse_args()

    run_pipeline(
        dry_run=args.dry_run,
        from_step=args.from_step,
        log_file=args.log_file,
        input_mode=args.input_mode,
    )