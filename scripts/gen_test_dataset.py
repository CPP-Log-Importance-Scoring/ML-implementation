"""
scripts/gen_test_dataset.py

Generate a RECENT-dated RFC 3164 syslog dataset for end-to-end testing of the
pipeline + dashboard. Unlike scripts/generate_real_logs.py (which hardcodes
2026-05-30 and so falls outside the dashboard's default 7-day window), this
script spans the last N days up to "now", so incidents appear in the default
Incident Feed view immediately.

It reuses the canonical normal/anomaly message patterns from
scripts.generate_real_logs for format fidelity, and additionally injects a
handful of tight, host-local "incident bursts" (a dense cluster of correlated
anomaly templates within a few minutes) so the DBSCAN incident clusterer,
correlation graph, and root-cause engine have meaningful structure to surface.

Usage
-----
    python scripts/gen_test_dataset.py                 # 3 days, ~5k lines
    python scripts/gen_test_dataset.py --days 2 --out data/raw/test_dataset.log

Then run the pipeline against the output file:
    python pipeline.py --log-file data/raw/test_dataset.log --input-mode syslog
"""
from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timedelta

from scripts.generate_real_logs import (
    _HOSTS,
    _NORMAL_PATTERNS,
    _ANOMALY_PATTERNS,
    _rand_ip,
    _rand_mac,
)

DEFAULT_OUT = "data/raw/test_dataset.log"

# A correlated failure cascade: an interface goes down and drags routing
# adjacencies with it. Ordered so the first line is the natural root cause.
_INCIDENT_CASCADE = [
    ("ifmgrd", "Interface 1/1/{port} changed state to down",            "<131>"),
    ("ifmgrd", "Interface 1/1/{port} link flap detected on uplink",     "<130>"),
    ("ospf",   "OSPF adjacency lost on vlan{vlan}",                      "<131>"),
    ("ospf",   "Neighbor {ip}/0 changed state to Down",                 "<131>"),
    ("bgp",    "BGP neighbor {ip} connection reset hold timer expired", "<130>"),
    ("bgp",    "BGP neighbor {ip} session reset by peer",               "<130>"),
    ("ifmgrd", "packet drop rate high on interface 1/1/{port}",         "<132>"),
    ("kernel", "CPU utilization {pct}% exceeded threshold alert",       "<132>"),
]


def _fmt(rng: random.Random, svc: str, tmpl: str, pri: str, host: str,
         ts: datetime, pid: int) -> str:
    msg = tmpl.format(
        ip=_rand_ip(rng), mac=_rand_mac(rng),
        port=rng.randint(1, 48), vlan=rng.randint(1, 4094),
        pct=rng.randint(70, 99),
    )
    return f"{pri}{ts.strftime('%b %d %H:%M:%S')} {host} {svc}[{pid}]: {msg}"


def generate(days: int = 3, seed: int = 7, anomaly_rate: float = 0.08,
             n_incidents: int = 6, mean_gap: float = 420.0) -> list[str]:
    rng = random.Random(seed)
    end = datetime.now().replace(microsecond=0)
    start = end - timedelta(days=days)
    rows: list[tuple[datetime, str]] = []

    # ── Baseline traffic: scattered normal + occasional lone anomalies ──────
    for host in _HOSTS:
        pid_base = rng.randint(1000, 9000)
        ts = start + timedelta(seconds=rng.uniform(0, 600))
        while ts < end:
            ts += timedelta(seconds=rng.expovariate(1 / mean_gap))  # mean ~mean_gap s/host
            if ts >= end:
                break
            pid = pid_base + rng.randint(0, 50)
            if rng.random() < anomaly_rate:
                svc, tmpl, pri = rng.choice(_ANOMALY_PATTERNS)
            else:
                svc, tmpl, pri = rng.choice(_NORMAL_PATTERNS)
            rows.append((ts, _fmt(rng, svc, tmpl, pri, host, ts, pid)))

    # ── Injected incident bursts: tight correlated cascades, recent-weighted ─
    for _ in range(n_incidents):
        host = rng.choice(_HOSTS)
        # Bias incidents toward the most recent ~40% of the window.
        frac = rng.uniform(0.6, 0.98)
        t0 = start + (end - start) * frac
        pid = rng.randint(1000, 9000)
        ts = t0
        for svc, tmpl, pri in _INCIDENT_CASCADE:
            # repeat each cascade stage a few times to build density/co-occurrence
            for _ in range(rng.randint(2, 4)):
                ts += timedelta(seconds=rng.uniform(1, 8))
                rows.append((ts, _fmt(rng, svc, tmpl, pri, host, ts, pid)))

    rows.sort(key=lambda r: r[0])
    return [line for _, line in rows]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=3, help="window length in days (default 3)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--incidents", type=int, default=6, help="injected correlated bursts")
    ap.add_argument("--mean-gap", type=float, default=420.0,
                    help="mean seconds between baseline events per host (default 420 ≈ ~8k lines/3d)")
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()

    lines = generate(days=args.days, seed=args.seed, n_incidents=args.incidents,
                     mean_gap=args.mean_gap)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    anom = sum(1 for ln in lines if any(p in ln for p in ("<130>", "<131>", "<132>")))
    print(f"Wrote {len(lines):,} syslog lines → {args.out}")
    print(f"  Window     : last {args.days} day(s), ending {datetime.now():%Y-%m-%d %H:%M}")
    print(f"  Hosts      : {len(_HOSTS)}")
    print(f"  Anomaly-ish : {anom:,} lines ({anom / max(len(lines),1):.1%}) incl. {args.incidents} injected bursts")


if __name__ == "__main__":
    main()
