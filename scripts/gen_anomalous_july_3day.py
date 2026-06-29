"""
scripts/gen_anomalous_july_3day.py
==================================
Generate a 3-day ANOMALOUS synthetic dataset (Jul 01-03, 2026), each day
carrying MULTIPLE correlated incident cascades, plus a ground-truth incident
report (Markdown) describing exactly what was injected.

Reuses the proven 7-section helpers from gen_network_logs.py so the output is
byte-compatible with parsing/synthetic_dataset_loader.py (--input-mode synthetic).

Usage:
    python scripts/gen_anomalous_july_3day.py
    python scripts/gen_anomalous_july_3day.py --out data/raw/anomalous_july2026 \
        --report data/raw/anomalous_july2026/INCIDENT_REPORT.md
"""
from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timedelta

from scripts.gen_network_logs import (
    DayLog,
    populate_normal,
    inject_incident,
    inject_drift,
)

SEED = 20260701  # distinct from train (20260601) and test (20260608)

# Human-readable descriptions for each incident kind (for the report).
KIND_DESC = {
    "interface_flap": "Physical interface flapping (carrier lost/restored repeatedly), dragging an OSPF adjacency down",
    "ospf_storm":     "OSPF reconvergence storm — multiple neighbors drop, SPF recomputes repeatedly, routing table unstable",
    "bgp_drop":       "BGP session drop on hold-timer expiry, thousands of prefixes withdrawn",
    "lacp_degraded":  "LACP bond degraded — member links removed on partner timeout, reduced aggregate capacity",
    "asic_ecc":       "ASIC uncorrectable ECC memory errors, forwarding plane degraded with packet drops",
    "psu_fault":      "Power-supply failure, redundancy lost",
    "oom":            "Memory pressure escalating into an OOM-kill cascade, a control-plane daemon SIGKILLed and restarted",
}
DRIFT_DESC = {
    "memleak":  "Gradual memory leak — heap climbs ~48%->96% over the day, terminating in a late OOM-kill cascade",
    "droprate": "Gradual egress drop-rate creep — congestion builds over hours into a tail-drop / forwarding-degraded cascade",
    "thermal":  "Gradual thermal creep — inlet temperature rises until thermal throttling / shutdown-imminent late in the day",
}

# Per-day plan: a varied mix so each day has multiple distinct incidents.
# Each entry is ("incident", kind) or ("drift", kind).
DAY_PLANS = {
    0: [  # Jul 01 — control-plane heavy day
        ("incident", "ospf_storm"),
        ("incident", "interface_flap"),
        ("incident", "bgp_drop"),
        ("incident", "asic_ecc"),
    ],
    1: [  # Jul 02 — system/hardware heavy day
        ("incident", "oom"),
        ("incident", "psu_fault"),
        ("incident", "lacp_degraded"),
        ("incident", "asic_ecc"),
        ("incident", "interface_flap"),
    ],
    2: [  # Jul 03 — discrete incidents + a slow drift
        ("incident", "bgp_drop"),
        ("incident", "ospf_storm"),
        ("drift", "memleak"),
    ],
}

SEV_RANK = {"INFO": 0, "WARNING": 1, "ERROR": 2, "CRITICAL": 3}


def _spread_hours(n: int, rng: random.Random) -> list[int]:
    """Pick n distinct, well-separated start hours within 01:00-22:00."""
    # carve the active window into n bands and jitter within each band
    lo, hi = 1, 22
    band = (hi - lo) / n
    hours = []
    for i in range(n):
        base = lo + band * i
        hours.append(int(min(hi, base + rng.uniform(0, band * 0.7))))
    return hours


def build_day(date: datetime, plan: list[tuple[str, str]], num: int,
              rng: random.Random) -> tuple[DayLog, list[dict]]:
    day = DayLog(
        date, num,
        f"Anomalous Day {date:%b %d} - Multiple Incidents",
        "CRITICAL",
        "Multiple correlated incident cascades across subsystems in one day",
        "Severe Multi-Incident",
        "Multiple incidents across subsystems",
    )
    # realistic but lighter normal backdrop so the incidents stand out
    populate_normal(day, rng, weekend=False, maintenance=False,
                    syslog_n=2200, s1_n=110, s3_n=90, s5_n=40)

    discrete = [(t, k) for (t, k) in plan if t == "incident"]
    hours = sorted(_spread_hours(len(discrete), rng))

    records: list[dict] = []
    all_aff: set[str] = set()
    all_sig: list[str] = []
    max_sev = "ERROR"

    for (kind, hour) in zip([k for (_, k) in discrete], hours):
        start = date + timedelta(hours=hour, minutes=rng.randint(0, 59),
                                 seconds=rng.randint(0, 59))
        lbl, aff, sig = inject_incident(day, rng, kind, start)
        sev = "CRITICAL" if kind in ("ospf_storm", "asic_ecc", "psu_fault", "oom") else "ERROR"
        if SEV_RANK[sev] > SEV_RANK[max_sev]:
            max_sev = sev
        all_aff.update(aff); all_sig += sig
        records.append({
            "label": lbl, "kind": kind, "start": start,
            "severity": sev, "affected": aff, "signals": sig,
            "desc": KIND_DESC[kind],
        })

    # optional drift component (spans the whole day, terminal breach late)
    for (t, kind) in plan:
        if t != "drift":
            continue
        lbl, aff, sig = inject_drift(day, rng, kind)
        max_sev = "CRITICAL"
        all_aff.update(aff); all_sig += sig
        records.append({
            "label": lbl, "kind": kind, "start": None,  # all-day drift
            "severity": "CRITICAL", "affected": aff, "signals": sig,
            "desc": DRIFT_DESC[kind],
        })

    # finalize Section 6/7 metadata
    day.training_label = "DENSE_MULTI_INCIDENT"
    day.failure_mode = "dense_multi_incident"
    day.severity = max_sev
    day.affected = sorted(all_aff)
    day.signals = list(dict.fromkeys(all_sig))
    day.root_cause = "Multiple correlated incidents - see per-incident cascades"
    day.root_chain = " -> ".join(list(dict.fromkeys(all_sig))[:12])

    records.sort(key=lambda r: (r["start"] or date))
    return day, records


def write_report(report_path: str, day_records: list[tuple[datetime, list[dict]]]):
    lines: list[str] = []
    lines.append("# Anomalous Dataset — Incident Report")
    lines.append("")
    lines.append("**Dataset:** 3 days, Jul 01–03 2026 (synthetic, 7-section, vendor-neutral)")
    total = sum(len(recs) for _, recs in day_records)
    lines.append(f"**Total incidents:** {total} across 3 days")
    lines.append("")
    lines.append("Each day below is a single `.log` file containing a full day of normal "
                 "background activity plus the injected incidents listed. Drift incidents "
                 "span the whole day; discrete incidents fire at the listed start time. "
                 "Ground-truth markers also appear inline in Sections 6 & 7 of each file.")
    lines.append("")

    for date, recs in day_records:
        fname = f"daily_{date:%Y-%m-%d}.log"
        lines.append(f"## {date:%A, %b %d %Y} — `{fname}`")
        lines.append("")
        lines.append(f"**{len(recs)} incidents**")
        lines.append("")
        lines.append("| # | Time | Severity | Incident | Affected | What happened |")
        lines.append("|---|------|----------|----------|----------|---------------|")
        for i, r in enumerate(recs, 1):
            when = f"{r['start']:%H:%M}" if r["start"] else "all-day (drift)"
            aff = ", ".join(r["affected"])
            lines.append(
                f"| {i} | {when} | {r['severity']} | **{r['label']}** | {aff} | {r['desc']} |"
            )
        lines.append("")
        # signal summary
        sigs: list[str] = []
        for r in recs:
            sigs += r["signals"]
        sigs = list(dict.fromkeys(sigs))
        lines.append(f"**Correlation signals present:** {', '.join(sigs)}")
        lines.append("")

    with open(report_path, "w") as fh:
        fh.write("\n".join(lines) + "\n")


def main():
    ap = argparse.ArgumentParser(description="Generate 3-day anomalous July dataset + report.")
    ap.add_argument("--out", default="data/raw/anomalous_july2026")
    ap.add_argument("--report", default=None,
                    help="Report path (default: <out>/INCIDENT_REPORT.md)")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    report_path = args.report or os.path.join(args.out, "INCIDENT_REPORT.md")

    rng = random.Random(SEED)
    day_records: list[tuple[datetime, list[dict]]] = []
    for d in range(3):
        date = datetime(2026, 7, 1) + timedelta(days=d)
        day, records = build_day(date, DAY_PLANS[d], 90 + d, rng)
        path = os.path.join(args.out, f"daily_{date:%Y-%m-%d}.log")
        with open(path, "w") as fh:
            fh.write(day.render())
        day_records.append((date, records))
        print(f"[anomalous] wrote {len(records)} incidents -> {path}")

    write_report(report_path, day_records)
    print(f"[report] wrote incident report -> {report_path}")


if __name__ == "__main__":
    main()
