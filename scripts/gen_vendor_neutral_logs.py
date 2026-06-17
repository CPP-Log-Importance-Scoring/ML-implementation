"""
scripts/gen_vendor_neutral_logs.py

Generate brand-new, VENDOR-NEUTRAL synthetic scenario logs in the mentor's
7-section format (the same shape as data/raw/synthetic_logs/), verified to parse
through parsing/synthetic_dataset_loader.py and to yield real anomalies.

Why a dedicated generator
-------------------------
The 7-section format is NOT flat syslog — it is a structured document the
synthetic loader routes section-by-section:
  S1 structured events  -> explicit severity= drives event_weight (anomaly signal)
  S2/S5 syslog          -> severity inferred by keyword (CRITICAL, "link flap", ...)
  S3 debug/trace        -> time-only [HH:MM:SS.ms], inherits the # Duration: date
  S4 performance metrics-> [HH:MM:SS.ms] Entity: key=value ...
  S7 training metadata  -> oracle labels (never fed to the model)

All component names are generic (spanning_tree_daemon, forwarding_engine, ...) —
no OSPF/BGP/HPE/vendor references — matching common/config.SERVICE_ALIAS_MAP.

Output is RECENT-dated (spread across the last few days, ending ~now) so the
incidents land inside the dashboard's default 7-day window.

Usage
-----
    python scripts/gen_vendor_neutral_logs.py                  # 6 scenarios
    python scripts/gen_vendor_neutral_logs.py --count 10 --out data/raw/synthetic_logs_generated

Then run the pipeline on the directory with the synthetic input mode:
    python pipeline.py --log-file data/raw/synthetic_logs_generated --input-mode synthetic
"""
from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timedelta

DEFAULT_OUT = "data/raw/synthetic_logs_generated"

# ── Scenario recipes ────────────────────────────────────────────────────────
# Each recipe is a self-contained vendor-neutral failure story. `events` are the
# Section-1 structured events (the high-severity anomaly signals); `cascade` are
# Section-2 incident syslog lines whose severity is inferred by keyword.
RECIPES = [
    {
        "key": "redundancy_split_brain", "title": "Redundancy Split-Brain (ISL Failure)",
        "severity": "CRITICAL", "failure_type": "Redundancy / High Availability Failure",
        "root_cause": "Inter-switch link failure causing peer isolation and dual-root election",
        "impact": "Service loss, duplicate root detection, traffic blackhole",
        "subsys": "REDUNDANCY", "daemon": "redundancy_daemon", "debug_file": "redundancy.c",
        "duration_s": 525, "training_label": "CRITICAL_HA_FAILURE", "failure_mode": "split_brain",
        "events": [
            ("REDUNDANCY_ISL_DOWN", "CRITICAL", {"isl_port": "backup_link_2", "peer_id": "SECONDARY"}),
            ("PEER_SYNC_LOST", "CRITICAL", {"peer_id": "SECONDARY", "sync_state": "LOST"}),
            ("SPLIT_BRAIN_DETECTED", "CRITICAL", {"peer_count": "2", "root_count": "2"}),
            ("DUPLICATE_ROOT_ALERT", "CRITICAL", {"component": "STP"}),
            ("REDUNDANCY_RECOVERY", "INFO", {"isl_port": "backup_link_2", "peer_id": "SECONDARY"}),
        ],
        "cascade": [
            ("redundancy_daemon", "CRITICAL - ISL link backup_link_2 DOWN"),
            ("sync_monitor", "Peer synchronization timeout - redundancy synchronization LOST"),
            ("redundancy_daemon", "CRITICAL - Split-brain condition DETECTED"),
            ("spanning_tree_daemon", "CRITICAL ALERT - Duplicate root device detected"),
            ("topology_monitor", "Network topology UNSTABLE - dual root condition"),
        ],
        "metrics": ("PeerSync", ["sync_latency", "peer_reachability", "root_count"]),
        "signals": ["ISL_DOWN", "SYNC_LOST", "DUPLICATE_ROOT", "SPLIT_BRAIN"],
        "affected": ["redundancy_daemon", "spanning_tree_daemon", "topology_monitor"],
    },
    {
        "key": "memory_pressure", "title": "Memory Pressure (Daemon OOM)",
        "severity": "CRITICAL", "failure_type": "Resource Exhaustion / Out-of-Memory",
        "root_cause": "Memory leak in forwarding and routing daemons exhausting system memory",
        "impact": "2 daemon OOM kills, forwarding interruption",
        "subsys": "FORWARDING", "daemon": "forwarding_engine", "debug_file": "forwarding.c",
        "duration_s": 450, "training_label": "CRITICAL_RESOURCE_FAILURE", "failure_mode": "oom",
        "events": [
            ("MEMORY_GROWTH_DETECTED", "WARN", {"daemon": "forwarding_engine", "rss_mb": "820"}),
            ("MEMORY_THRESHOLD_EXCEEDED", "ERROR", {"daemon": "forwarding_engine", "rss_mb": "1180"}),
            ("OOM_KILLER_INVOCATION", "CRITICAL", {"daemon": "forwarding_engine"}),
            ("DAEMON_TERMINATION", "CRITICAL", {"daemon": "routing_daemon", "signal": "SIGKILL"}),
            ("DAEMON_RESTART", "INFO", {"daemon": "forwarding_engine"}),
        ],
        "cascade": [
            ("process_monitor", "Memory usage 88% threshold exceeded"),
            ("process_monitor", "Memory usage 96% threshold exceeded critical"),
            ("forwarding_engine", "CRITICAL - out of memory, invoking OOM killer"),
            ("routing_daemon", "CRITICAL - daemon terminated by OOM killer"),
            ("process_monitor", "forwarding_engine restart in progress"),
        ],
        "metrics": ("Memory", ["used_mb", "free_mb", "swap_mb"]),
        "signals": ["MEMORY_GROWTH", "OOM_KILLER_INVOCATION", "DAEMON_TERMINATION"],
        "affected": ["forwarding_engine", "routing_daemon", "process_monitor"],
    },
    {
        "key": "cpu_overload", "title": "CPU Overload (ACL Processing Spike)",
        "severity": "HIGH", "failure_type": "CPU Resource Exhaustion",
        "root_cause": "Spike in access-control rule processing workload saturating CPU",
        "impact": "Latency increase, task processing collapse",
        "subsys": "ACL", "daemon": "access_control_daemon", "debug_file": "acl.c",
        "duration_s": 300, "training_label": "HIGH_CPU_FAILURE", "failure_mode": "cpu_saturation",
        "events": [
            ("CPU_SPIKE", "HIGH", {"util_pct": "94"}),
            ("LOAD_AVERAGE_SPIKE", "HIGH", {"load_1m": "12.5"}),
            ("CONTEXT_SWITCH_RATE_SPIKE", "WARN", {"cs_per_s": "48000"}),
            ("TASK_PROCESSING_DEGRADED", "ERROR", {"tasks_per_s": "5"}),
            ("CPU_RECOVERY", "INFO", {"util_pct": "31"}),
        ],
        "cascade": [
            ("statistics_collector", "CPU utilization 94% exceeded threshold alert"),
            ("access_control_daemon", "ACL error rule processing backlog growing"),
            ("process_monitor", "CRITICAL - task scheduler starvation detected"),
            ("statistics_collector", "CPU utilization 97% exceeded threshold alert"),
            ("statistics_collector", "CPU utilization 31% normal"),
        ],
        "metrics": ("CPU", ["utilization", "load_1m", "context_switches"]),
        "signals": ["CPU_SPIKE", "LOAD_AVERAGE_SPIKE", "CONTEXT_SWITCH_RATE_SPIKE"],
        "affected": ["access_control_daemon", "statistics_collector", "process_monitor"],
    },
    {
        "key": "buffer_congestion", "title": "Buffer Congestion (Egress Overflow)",
        "severity": "HIGH", "failure_type": "Queue Management / Buffer Overflow",
        "root_cause": "Ingress traffic spike exceeding egress capacity",
        "impact": "Peak drop rate, thousands of packets lost",
        "subsys": "BUFFER", "daemon": "buffer_manager", "debug_file": "buffer.c",
        "duration_s": 240, "training_label": "HIGH_BUFFER_FAILURE", "failure_mode": "congestion",
        "events": [
            ("TRAFFIC_SPIKE", "WARN", {"ingress_pps": "182000"}),
            ("BUFFER_PRESSURE", "HIGH", {"util_pct": "91"}),
            ("QUEUE_BUILDUP", "HIGH", {"queue_depth": "980"}),
            ("EGRESS_DROPS", "ERROR", {"drop_pps": "5234"}),
            ("BUFFER_RECOVERY", "INFO", {"util_pct": "22"}),
        ],
        "cascade": [
            ("buffer_manager", "packet drop rate high on egress queue 3"),
            ("qos_scheduler_daemon", "egress queue depth exceeding watermark"),
            ("buffer_manager", "CRITICAL - buffer exhaustion, tail-dropping packets"),
            ("statistics_collector", "drop rate high 5234 pps sustained"),
            ("buffer_manager", "buffer utilization 22% normal"),
        ],
        "metrics": ("Buffer", ["utilization", "queue_depth", "drop_pps"]),
        "signals": ["BUFFER_PRESSURE", "QUEUE_BUILDUP", "EGRESS_DROPS", "TRAFFIC_SPIKE"],
        "affected": ["buffer_manager", "qos_scheduler_daemon", "statistics_collector"],
    },
    {
        "key": "interface_errors", "title": "Interface Errors (Physical Degradation)",
        "severity": "MEDIUM", "failure_type": "Physical Layer Degradation",
        "root_cause": "Transceiver degradation or cable alignment issue",
        "impact": "Rising CRC/FCS errors, throughput reduction",
        "subsys": "PHYSICAL", "daemon": "physical_monitor", "debug_file": "physical.c",
        "duration_s": 600, "training_label": "MEDIUM_PHYSICAL_FAILURE", "failure_mode": "phy_degradation",
        "events": [
            ("CRC_ERRORS", "MEDIUM", {"count": "67", "port": "1/1/14"}),
            ("FCS_ERRORS", "MEDIUM", {"count": "152", "port": "1/1/14"}),
            ("ALIGNMENT_ERRORS", "WARN", {"count": "140", "port": "1/1/14"}),
            ("SIGNAL_DEGRADATION", "MEDIUM", {"rx_power_dbm": "-9.8"}),
            ("ERROR_RATE_STABILIZED", "INFO", {"port": "1/1/14"}),
        ],
        "cascade": [
            ("physical_monitor", "interface 1/1/14 CRC error count rising"),
            ("physical_monitor", "interface 1/1/14 FCS error threshold exceeded"),
            ("statistics_collector", "alignment errors detected on port 1/1/14"),
            ("physical_monitor", "signal degradation rx power below threshold"),
            ("physical_monitor", "interface 1/1/14 error rate stabilized"),
        ],
        "metrics": ("Interface", ["crc_errors", "fcs_errors", "rx_power_dbm"]),
        "signals": ["CRC_ERRORS", "FCS_ERRORS", "ALIGNMENT_ERRORS", "SIGNAL_DEGRADATION"],
        "affected": ["physical_monitor", "statistics_collector"],
    },
    {
        "key": "routing_flap", "title": "Routing Session Flapping",
        "severity": "HIGH", "failure_type": "Routing Protocol Instability",
        "root_cause": "Link-layer instability on a routing peer link causing repeated session resets",
        "impact": "Repeated flaps, convergence delay, routes suppressed",
        "subsys": "ROUTING", "daemon": "routing_daemon", "debug_file": "routing.c",
        "duration_s": 750, "training_label": "HIGH_ROUTING_FAILURE", "failure_mode": "route_flap",
        "events": [
            ("ROUTING_SESSION_DOWN", "ERROR", {"peer_id": "PEER_A"}),
            ("ROUTING_SESSION_UP", "INFO", {"peer_id": "PEER_A"}),
            ("FLAP_RATE_INCREASE", "HIGH", {"flaps": "35", "window_s": "750"}),
            ("ROUTE_SUPPRESSION", "HIGH", {"suppressed": "15"}),
            ("CONVERGENCE_RESTORED", "INFO", {"peer_id": "PEER_A"}),
        ],
        "cascade": [
            ("routing_daemon", "routing session reset by peer PEER_A"),
            ("routing_daemon", "routing session re-established with PEER_A"),
            ("network_monitor", "link flap detected on peer uplink"),
            ("routing_daemon", "CRITICAL - route suppression engaged, 15 routes damped"),
            ("routing_daemon", "convergence restored, sessions stable"),
        ],
        "metrics": ("Routing", ["flap_count", "convergence_ms", "suppressed_routes"]),
        "signals": ["ROUTING_SESSION_DOWN_UP", "FLAP_RATE_INCREASE", "ROUTE_SUPPRESSION"],
        "affected": ["routing_daemon", "network_monitor"],
    },
]

# Baseline heartbeat services (the ~90% benign noise) — vendor-neutral.
_BASELINE = [
    ("monitoring_active", "Continuous monitoring active"),
    ("baseline_check", "All subsystem metrics nominal"),
    ("peer_heartbeat", "Peer heartbeat normal"),
    ("topology_monitor", "Network topology stable"),
    ("health_check", "System health check passed all subsystems"),
    ("statistics_collector", "Periodic statistics collection complete"),
]


def _iso(dt: datetime, tz: bool = False) -> str:
    s = dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"
    return s + ("+00:00" if tz else "Z")


def _gen_scenario(idx: int, recipe: dict, base: datetime, rng: random.Random,
                  baseline_pad: int) -> str:
    L: list[str] = []
    dur = recipe["duration_s"]
    start, end = base, base + timedelta(seconds=dur)
    ev_id0 = 1000 * (idx + 1)

    # ── Header ──────────────────────────────────────────────────────────────
    L.append(f"# SYNTHETIC LOG DATASET - Scenario {idx:02d}: {recipe['title']} ({recipe['severity']})")
    L.append("# WARNING: SYNTHETIC, VENDOR-NEUTRAL dataset for training purposes ONLY")
    L.append(f"# Scenario: {recipe['root_cause']}")
    L.append(f"# Failure Type: {recipe['failure_type']}")
    L.append(f"# Severity: {recipe['severity']}")
    L.append(f"# Duration: {start:%Y-%m-%d %H:%M:%S} to {end:%H:%M:%S} ({dur} seconds)")
    L.append(f"# Impact: {recipe['impact']}")
    L.append("")

    # ── Section 1: structured events ────────────────────────────────────────
    L.append("## SECTION 1: STRUCTURED EVENTS")
    n_ev = len(recipe["events"])
    for i, (name, sev, extra) in enumerate(recipe["events"]):
        ts = start + timedelta(seconds=dur * (i / max(n_ev - 1, 1)) * 0.9 + rng.uniform(0, 2))
        comp = extra.pop("component", recipe["subsys"])
        kv = " | ".join(f"{k}={v}" for k, v in extra.items())
        ms = int(ts.timestamp() * 1000)
        tail = f"event_id={ev_id0 + i} | " + (f"{kv} | " if kv else "") + f"timestamp_ms={ms}"
        L.append(f"[{_iso(ts)}] EVENT: {name} | severity={sev} | component={comp} |")
        L.append(f"   {tail}")
        L.append("")

    # ── Section 2: syslog (baseline → incident cascade → recovery) ──────────
    L.append("## SECTION 2: SYSLOG ENTRIES")
    pre = start - timedelta(seconds=baseline_pad * 30)
    for j in range(baseline_pad):
        svc, msg = rng.choice(_BASELINE)
        L.append(f"{_iso(pre + timedelta(seconds=j * 30), tz=True)} network_device {svc}: {msg}")
    n_c = len(recipe["cascade"])
    for i, (svc, msg) in enumerate(recipe["cascade"]):
        ts = start + timedelta(seconds=dur * (i / max(n_c - 1, 1)) * 0.95 + rng.uniform(0, 1))
        L.append(f"{_iso(ts, tz=True)} network_device {svc}: {msg}")
    for j in range(baseline_pad):
        svc, msg = rng.choice(_BASELINE)
        L.append(f"{_iso(end + timedelta(seconds=j * 30), tz=True)} network_device {svc}: {msg}")
    L.append("")

    # ── Section 3: debug/trace (time-only, inherits Duration date) ──────────
    L.append("## SECTION 3: DEBUG/TRACE LOGS")
    funcs = ["state_handler", "event_dispatch", "failover_init", "recovery_step", "convergence_check"]
    for i in range(8):
        ts = start + timedelta(seconds=dur * (i / 8) + rng.uniform(0, 1))
        line_no = rng.randint(100, 900)
        fn = rng.choice(funcs)
        L.append(f"[{ts:%H:%M:%S}.{ts.microsecond // 1000:03d}] {recipe['debug_file']}:{line_no} | {fn}: step {i + 1} processed")
    L.append("")

    # ── Section 4: performance metrics (time-only) ──────────────────────────
    L.append("## SECTION 4: PERFORMANCE METRICS")
    entity, metric_names = recipe["metrics"]
    for i in range(10):
        ts = start + timedelta(seconds=dur * (i / 10))
        ramp = i / 9.0  # 0..1 degradation ramp
        kvs = []
        for mn in metric_names:
            base_v = rng.uniform(10, 40)
            val = round(base_v + ramp * rng.uniform(40, 120), 2)
            kvs.append(f"{mn}={val}")
        L.append(f"[{ts:%H:%M:%S}.{ts.microsecond // 1000:03d}] {entity}: {' '.join(kvs)}")
    L.append("")

    # ── Section 5: additional contextual syslog ─────────────────────────────
    L.append("## SECTION 5: ADDITIONAL CONTEXTUAL LOGS")
    for i in range(4):
        ts = end + timedelta(seconds=30 * (i + 1))
        L.append(f"{_iso(ts, tz=True)} network_device system_logger: post-incident checkpoint {i + 1}")
    L.append("")

    # ── Section 6: correlation analysis (free text; parser ignores) ─────────
    L.append("## SECTION 6: CORRELATION ANALYSIS")
    L.append(f"Root cause chain: {' -> '.join(recipe['signals'])}")
    L.append(f"Primary affected components: {', '.join(recipe['affected'])}")
    L.append("")

    # ── Section 7: training metadata (oracle labels) ────────────────────────
    L.append("## SECTION 7: TRAINING METADATA")
    L.append(f"training_label: {recipe['training_label']}")
    L.append(f"failure_mode: {recipe['failure_mode']}")
    L.append(f"root_cause: {recipe['root_cause']}")
    L.append(f"file_severity: {recipe['severity']}")
    L.append(f"affected_components: [{', '.join(recipe['affected'])}]")
    L.append(f"correlation_signals: [{', '.join(recipe['signals'])}]")
    L.append("")
    return "\n".join(L) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--count", type=int, default=len(RECIPES),
                    help=f"number of scenario files (1..{len(RECIPES)}, default all)")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--baseline-pad", type=int, default=8,
                    help="benign heartbeat lines before & after each incident")
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    os.makedirs(args.out, exist_ok=True)
    count = max(1, min(args.count, len(RECIPES)))
    now = datetime.now().replace(microsecond=0)

    written = []
    for i in range(count):
        recipe = RECIPES[i]
        # Spread incidents across the last ~5 days, ending within the last day.
        base = now - timedelta(days=(count - i) * 0.8, hours=rng.uniform(0, 6))
        base = base.replace(microsecond=0)
        text = _gen_scenario(i + 1, recipe, base, rng, args.baseline_pad)
        path = os.path.join(args.out, f"scenario_{i + 1:02d}_{recipe['key']}.log")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
        written.append((path, recipe["severity"], base))

    print(f"Wrote {len(written)} vendor-neutral scenario files → {args.out}/")
    for p, sev, base in written:
        print(f"  {sev:8} {base:%Y-%m-%d %H:%M}  {os.path.basename(p)}")


if __name__ == "__main__":
    main()
