#!/usr/bin/env python3
"""
gen_sim_real_dataset.py
=======================
Generate a production-like corpus for a train -> held-out-test experiment.

Layout (under data/raw/sim_real/):
  train/         mostly-clean training corpus  (14 clean + 2 anomaly days)
  test_clean/    held-out clean days           (3)
  test_anomaly/  held-out anomaly days         (3, NOVEL scenarios not in train)

"Real-ish" ratio: ~12% of training days carry an incident. Test anomaly days
deliberately use scenario types ABSENT from training (PROTOCOL_STARVATION,
OOM_KILL_CASCADE, SPLIT_BRAIN) so detection is measured on genuinely unseen
failure modes, not memorised ones.

Reuses the 7-section base generator so output is byte-compatible with the
synthetic loader (input-mode synthetic) and carries `# [ANOMALY_START ...]`
ground-truth markers on anomaly days.
"""

import copy
import os
import sys
from datetime import date, datetime, timedelta

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "data", "raw", "claude_generated"))
import generate_daily_logs as base  # noqa: E402

OUT = os.path.join(_ROOT, "data", "raw", "sim_real")

_BASE_MEMORY = {
    "Monday": 48, "Tuesday": 48, "Wednesday": 52, "Thursday": 50,
    "Friday": 51, "Saturday": 49, "Sunday": 50,
}

# DAYS index -> headline scenario (for logging)
SCENARIO = {0: "PROTOCOL_STARVATION", 1: "LINK_FLAPPING", 2: "BUFFER_CONGESTION",
            3: "OOM_KILL_CASCADE", 4: "BGP_ROUTE_FLAPPING", 5: "CPU_OVERLOAD",
            6: "SPLIT_BRAIN"}


def clean_cfg(d: date) -> dict:
    return {
        "date": d.strftime("%Y-%m-%d"),
        "dayname": d.strftime("%A"),
        "base_memory": _BASE_MEMORY.get(d.strftime("%A"), 50),
    }


def anomaly_cfg(d: date, idx: int) -> dict:
    cfg = copy.deepcopy(base.DAYS[idx])
    cfg["date"] = d.strftime("%Y-%m-%d")
    cfg["dayname"] = d.strftime("%A")
    return cfg


def write_set(subdir: str, specs: list[tuple[date, dict, str]]) -> int:
    outdir = os.path.join(OUT, subdir)
    os.makedirs(outdir, exist_ok=True)
    total = 0
    for d, cfg, tag in specs:
        lines = base.generate_day(cfg)
        path = os.path.join(outdir, f"daily_{cfg['date']}.log")
        with open(path, "w") as f:
            f.write("\n".join(lines) + "\n")
        print(f"  {subdir:13} {cfg['date']}  {len(lines):5} lines  {tag}")
        total += len(lines)
    return total


def main() -> None:
    start = date(2026, 4, 1)
    # --- TRAIN: 16 days, anomalies on day 7 (CPU_OVERLOAD) and 13 (LINK_FLAPPING)
    train_anom = {6: 5, 12: 1}          # day-offset -> DAYS idx
    train = []
    for i in range(16):
        d = start + timedelta(days=i)
        if i in train_anom:
            idx = train_anom[i]
            train.append((d, anomaly_cfg(d, idx), f"ANOMALY {SCENARIO[idx]}"))
        else:
            train.append((d, clean_cfg(d), "clean"))

    # --- TEST clean: next 3 days
    tclean = [(start + timedelta(days=16 + i),) for i in range(3)]
    tclean = [(d, clean_cfg(d), "clean") for (d,) in tclean]

    # --- TEST anomaly: 3 NOVEL scenarios (not used in train)
    test_anom_plan = [(19, 0), (20, 3), (21, 6)]   # day-offset -> DAYS idx
    tanom = []
    for off, idx in test_anom_plan:
        d = start + timedelta(days=off)
        tanom.append((d, anomaly_cfg(d, idx), f"ANOMALY {SCENARIO[idx]} (novel)"))

    print("Generating sim_real corpus...")
    n1 = write_set("train", train)
    n2 = write_set("test_clean", tclean)
    n3 = write_set("test_anomaly", tanom)
    print(f"\nTRAIN: 14 clean + 2 anomaly | TEST: 3 clean + 3 anomaly (novel)")
    print(f"Lines — train {n1:,}  test_clean {n2:,}  test_anomaly {n3:,}")


if __name__ == "__main__":
    main()
