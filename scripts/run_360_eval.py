"""
scripts/run_360_eval.py
=======================
Run the 4-scenario 360° evaluation of the anomaly detection model.

Scenarios
---------
  1. Anomalous data  ×  Baseline-only model      (v171339)
  2. Anomalous data  ×  Baseline+Anomaly model   (v195047)
  3. Clean data      ×  Baseline-only model      (v171339)
  4. Clean data      ×  Baseline+Anomaly model   (v195047)

For each scenario:
  - Clears data/processed/ and incident_history state
  - Freezes the retrain trigger (sets unprocessed_logs_count to a large number
    minus 1 so maybe_retrain() NEVER fires — model stays fixed)
  - Moves the non-target model .pkl/.json out of model_store temporarily
  - Runs pipeline --dry-run in synthetic (auto) mode
  - Captures oracle_report.txt and scored_logs_df label counts
  - Restores model_store to original state

Results are written to evaluation/results/360_eval_results.json.
"""
from __future__ import annotations

import glob
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODEL_STORE = ROOT / "ml" / "model_store"
PROCESSED = ROOT / "data" / "processed"
ORACLE_TXT = ROOT / "evaluation" / "results" / "oracle_report.txt"
RESULTS_JSON = ROOT / "evaluation" / "results" / "360_eval_results.json"

# ── Model identities ──────────────────────────────────────────────────────────
BASELINE_TS      = "20260626_171339"   # Sumukha's model — baseline-only
BASELINE_ANO_TS  = "20260626_195047"   # auto-retrained on anomalous pr_test data

# ── Large number used to suppress maybe_retrain() firing ─────────────────────
# retrain_state.json stores unprocessed_logs_count; trigger fires when count >= K.
# We read K from config and set count = K - 1 so it NEVER fires this run.
def _get_retrain_k() -> int:
    sys.path.insert(0, str(ROOT))
    try:
        from common.config import RETRAINING_TRIGGER_EVERY_K
        return int(RETRAINING_TRIGGER_EVERY_K)
    except Exception:
        return 999_999  # safe fallback


def reset_state() -> None:
    """Wipe all processed artefacts and incident history."""
    for p in PROCESSED.glob("*.parquet"):
        p.unlink(missing_ok=True)
    for p in PROCESSED.glob("*.json"):
        p.unlink(missing_ok=True)
    for p in PROCESSED.glob("*.gpickle"):
        p.unlink(missing_ok=True)
    # Reset retrain counter to 0 but freeze by setting to K-1 immediately below
    retrain_file = MODEL_STORE / "retrain_state.json"
    retrain_file.write_text(json.dumps({"unprocessed_logs_count": 0}))


def freeze_retrain() -> None:
    """Set unprocessed_logs_count = K-1 so maybe_retrain never fires."""
    k = _get_retrain_k()
    frozen = max(0, k - 1)
    retrain_file = MODEL_STORE / "retrain_state.json"
    retrain_file.write_text(json.dumps({"unprocessed_logs_count": frozen}))
    print(f"  [freeze] retrain_state set to {frozen} (K={k}, won't fire)")


def isolate_model(active_ts: str) -> dict:
    """
    Temporarily move all .pkl/.json model files that are NOT active_ts
    to a backup directory. Returns a dict of {backup_path: original_path}
    for restoration.
    """
    backup_dir = MODEL_STORE / "_backup"
    backup_dir.mkdir(exist_ok=True)
    moved: dict[Path, Path] = {}

    for p in MODEL_STORE.glob("isolation_forest_v*.pkl"):
        ts = p.stem.replace("isolation_forest_v", "")
        if ts != active_ts:
            dst = backup_dir / p.name
            shutil.move(str(p), str(dst))
            moved[dst] = p
            # also move the sidecar json
            json_src = p.with_suffix(".json")
            if json_src.exists():
                json_dst = backup_dir / json_src.name
                shutil.move(str(json_src), str(json_dst))
                moved[json_dst] = json_src
            print(f"  [isolate] moved {p.name} to backup")

    return moved


def restore_models(moved: dict) -> None:
    """Restore models moved to backup."""
    for src, dst in moved.items():
        shutil.move(str(src), str(dst))
    # clean up backup dir if empty
    backup_dir = MODEL_STORE / "_backup"
    try:
        backup_dir.rmdir()
    except OSError:
        pass


def run_pipeline(data_dir: str) -> tuple[int, str]:
    """Run pipeline --dry-run on data_dir (synthetic/auto mode). Returns (returncode, stdout)."""
    cmd = [
        sys.executable, str(ROOT / "pipeline.py"),
        "--dry-run",
        "--log-file", data_dir,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(ROOT))
    combined = result.stdout + result.stderr
    return result.returncode, combined


def parse_oracle(oracle_path: Path) -> dict:
    """Parse oracle_report.txt into a dict of metric -> value."""
    metrics: dict = {}
    if not oracle_path.exists():
        return metrics
    for line in oracle_path.read_text().splitlines():
        if ":" in line:
            k, _, v = line.partition(":")
            k, v = k.strip(), v.strip()
            try:
                metrics[k] = float(v) if "." in v else int(v)
            except ValueError:
                metrics[k] = v
    return metrics


def parse_label_counts(pipeline_output: str) -> dict:
    """Extract label counts from pipeline stdout."""
    counts = {}
    for line in pipeline_output.splitlines():
        for label in ("critical", "medium", "low", "ignore"):
            tag = f"Label {label}"
            if tag in line and "rows" in line:
                try:
                    parts = line.split()
                    idx = parts.index("rows")
                    counts[label] = int(parts[idx - 1])
                except (ValueError, IndexError):
                    pass
    return counts


def run_scenario(num: int, name: str, data_dir: str, model_ts: str) -> dict:
    print(f"\n{'='*60}")
    print(f"SCENARIO {num}: {name}")
    print(f"  Data:  {data_dir}")
    print(f"  Model: isolation_forest_v{model_ts}")
    print('='*60)

    reset_state()
    moved = isolate_model(model_ts)
    freeze_retrain()

    rc, output = run_pipeline(data_dir)
    oracle = parse_oracle(ORACLE_TXT)
    labels = parse_label_counts(output)

    restore_models(moved)

    result = {
        "scenario": num,
        "name": name,
        "data_dir": str(data_dir),
        "model": f"isolation_forest_v{model_ts}",
        "pipeline_exit_code": rc,
        "label_counts": labels,
        "oracle_metrics": oracle,
    }

    print(f"\n  Labels: {labels}")
    print(f"  Escalated incidents: {oracle.get('n_incidents_escalated', '?')}/{oracle.get('n_incidents', '?')}")
    print(f"  Anomaly Recall: {oracle.get('anomaly_recall', '?')}")
    print(f"  Escalated Precision: {oracle.get('escalated_incident_precision', '?')}")
    print(f"  Signal Recall: {oracle.get('incident_signal_recall', '?')}")
    return result


def main() -> None:
    anomalous_dir = str(ROOT / "data" / "raw" / "eval_anomalous")
    clean_dir     = str(ROOT / "data" / "raw" / "eval_clean")

    scenarios = [
        (1, "Anomalies after Baseline",   anomalous_dir, BASELINE_TS),
        (2, "Anomalies after Anomalies",  anomalous_dir, BASELINE_ANO_TS),
        (3, "Clean after Baseline",       clean_dir,     BASELINE_TS),
        (4, "Clean after Anomalies",      clean_dir,     BASELINE_ANO_TS),
    ]

    all_results = []
    for num, name, data_dir, model_ts in scenarios:
        result = run_scenario(num, name, data_dir, model_ts)
        all_results.append(result)

    RESULTS_JSON.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_JSON.write_text(json.dumps(all_results, indent=2))
    print(f"\n\nAll results saved -> {RESULTS_JSON}")

    # Restore full model_store (both models)
    print("\nModel store after evaluation:")
    for p in sorted(MODEL_STORE.glob("isolation_forest_v*")):
        print(f"  {p.name}")


if __name__ == "__main__":
    main()
