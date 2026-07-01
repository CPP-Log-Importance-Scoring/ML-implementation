"""
scripts/gen_eval_datasets.py
============================
Generate evaluation datasets for the 360° model evaluation.

Produces:
  data/raw/eval_anomalous/  — 5 days (Jul 11–15), one dominant anomaly scenario each
  data/raw/eval_clean/      — 5 days (Jul 16–20), pure healthy-baseline

Dates start July 11 to be non-overlapping with:
  - Training data  : June 2026
  - pr_test data   : July 6–10

Uses a distinct random seed (SEED=20260711) so generated content is
independent from both training and pr_test runs.

Anomaly scenarios covered (one per anomalous day):
  Day 1 (Jul 11): OSPF reconvergence storm          — routing plane failure
  Day 2 (Jul 12): Memory OOM cascade                — system/process failure
  Day 3 (Jul 13): ASIC ECC uncorrectable errors     — hardware/dataplane failure
  Day 4 (Jul 14): Gradual memory leak (drift)       — slow metric drift
  Day 5 (Jul 15): Gradual thermal creep (drift)     — environmental drift

Usage:
    python scripts/gen_eval_datasets.py
    python scripts/gen_eval_datasets.py --anomalous-out data/raw/eval_anomalous \
                                         --clean-out data/raw/eval_clean
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Re-use all helpers from the existing generator (same repo, same format)
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.gen_network_logs import (
    DayLog,
    populate_normal,
    inject_incident,
    inject_drift,
)

# Distinct seed — must not collide with gen_network_logs.SEED (20260601) or
# its test seed (20260601+7=20260608). 20260711 is the first eval date itself.
EVAL_SEED_ANOMALOUS = 20260711
EVAL_SEED_CLEAN = 20260716


def _finalize(day: DayLog, label: str, affected: list, signals: list, sev: str) -> None:
    day.training_label = label
    day.failure_mode = label.lower()
    day.severity = sev
    day.affected = affected
    day.signals = signals
    day.root_cause = f"{label} — see correlated cascade"
    day.root_chain = " -> ".join(signals)


def gen_anomalous(out_dir: str) -> None:
    """Generate 5 anomalous eval days (July 11–15), one scenario each."""
    import random
    os.makedirs(out_dir, exist_ok=True)
    rng = random.Random(EVAL_SEED_ANOMALOUS)

    scenarios = [
        # (date_offset, scenario_num, scenario_kind, drift, title, desc, impact)
        (0,  91, "ospf_storm",  False,
         "Single-Scenario: OSPF Reconvergence Storm",
         "Routing plane failure — OSPF adjacency mass-loss + SPF storm",
         "Loss of routing convergence, forwarding degraded"),
        (1,  92, "oom",         False,
         "Single-Scenario: Memory OOM Cascade",
         "System memory exhaustion — OOM kill of critical process",
         "Route daemon killed, brief routing disruption"),
        (2,  93, "asic_ecc",   False,
         "Single-Scenario: ASIC ECC Uncorrectable Errors",
         "Hardware dataplane failure — uncorrectable ECC memory errors",
         "Forwarding degraded, packet loss on affected memory region"),
        (3,  94, "memleak",    True,
         "Gradual Drift: Memory Leak to OOM",
         "Slow memory leak trending over 24h, terminal OOM breach at ~21h30",
         "Progressive degradation, late-day critical process kill"),
        (4,  95, "thermal",    True,
         "Gradual Drift: Thermal Creep to Shutdown",
         "Inlet temperature rising from 36C to 74C across the day",
         "Progressive thermal throttling, critical threshold at ~22h15"),
    ]

    base = datetime(2026, 7, 11)

    for offset, num, kind, is_drift, title, desc, impact in scenarios:
        date = base + timedelta(days=offset)
        sev = "CRITICAL" if kind in ("ospf_storm", "asic_ecc", "oom") else "ERROR"
        day = DayLog(date, num, title, sev, desc,
                     "Single Incident" if not is_drift else "Gradual Drift", impact)

        # Normal backdrop (lighter than training so anomalies stand out clearly)
        populate_normal(day, rng, weekend=False, maintenance=False,
                        syslog_n=2200, s1_n=110, s3_n=90, s5_n=40)

        if is_drift:
            lbl, aff, sig = inject_drift(day, rng, kind)
        else:
            start = date + timedelta(hours=rng.randint(8, 18),
                                     minutes=rng.randint(0, 59))
            lbl, aff, sig = inject_incident(day, rng, kind, start)

        _finalize(day, lbl, aff, sig, sev)
        fname = f"eval_anomalous_2026-07-{11 + offset:02d}.log"
        with open(os.path.join(out_dir, fname), "w") as fh:
            fh.write(day.render())
        print(f"  [anomalous] wrote {fname}  ({kind})")

    print(f"[gen_anomalous] 5 files -> {out_dir}")


def gen_clean(out_dir: str) -> None:
    """Generate 5 clean eval days (July 16–20), zero incidents."""
    import random
    os.makedirs(out_dir, exist_ok=True)
    rng = random.Random(EVAL_SEED_CLEAN)

    # day_offset, weekday, maintenance, label_suffix
    day_configs = [
        (0, False, False, "Weekday Steady-State"),           # Jul 16 Wed
        (1, False, False, "Weekday Steady-State"),           # Jul 17 Thu
        (2, True,  False, "Weekend Quiet"),                  # Jul 18 Sat
        (3, True,  False, "Weekend Quiet"),                  # Jul 19 Sun
        (4, False, True,  "Weekday with Maintenance Window"), # Jul 20 Mon
    ]
    base = datetime(2026, 7, 16)

    for offset, weekend, maintenance, label in day_configs:
        date = base + timedelta(days=offset)
        num = 96 + offset
        kind = "weekend" if weekend else ("maintenance" if maintenance else "weekday")
        sysn = 2600 if weekend else 4200
        day = DayLog(date, num,
                     f"Clean Baseline Day — {label}", "INFO",
                     f"Normal {kind} operation, full healthy-behaviour variety, no incidents",
                     "Normal Operation" + (" (planned maintenance window)" if maintenance else ""),
                     "None — zero incidents, zero alerts")
        populate_normal(day, rng, weekend=weekend, maintenance=maintenance, syslog_n=sysn)
        fname = f"eval_clean_2026-07-{16 + offset:02d}.log"
        with open(os.path.join(out_dir, fname), "w") as fh:
            fh.write(day.render())
        print(f"  [clean] wrote {fname}  ({kind})")

    print(f"[gen_clean] 5 files -> {out_dir}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate 360° eval datasets (Jul 11–20).")
    ap.add_argument("--anomalous-out", default="data/raw/eval_anomalous")
    ap.add_argument("--clean-out",     default="data/raw/eval_clean")
    args = ap.parse_args()
    gen_anomalous(args.anomalous_out)
    gen_clean(args.clean_out)
    print("\nDone. Non-overlapping with June training and July 6–10 pr_test data.")


if __name__ == "__main__":
    main()
