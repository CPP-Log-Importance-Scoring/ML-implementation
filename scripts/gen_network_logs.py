"""
scripts/gen_network_logs.py
===========================
Generate 7-section synthetic network-device logs (the format consumed by
parsing/synthetic_dataset_loader.py via `--input-mode synthetic`).

Design goals
------------
1. CLEAN TRAINING DATA that covers the full envelope of *normal* network-device
   behaviour, so the IsolationForest baseline learns "healthy" with enough
   variety that a busy-but-normal day is not flagged. Variety axes baked in:
     * diurnal cycle      — traffic/CPU/util rise in business hours, fall at night
     * weekday vs weekend — weekends quieter, fewer config changes / logins
     * periodic jobs      — NTP sync, SNMP polls, nightly config backup
     * maintenance windows— a couple of nights with extra (still-NORMAL) activity:
                            planned config pushes, admin sessions, interface work
   Clean days are INFO-only (rare benign WARN), ZERO error/critical — no
   contamination of the training baseline.

2. TEST DATA in three flavours the user asked for:
     * dense   — many distinct incident cascades in one day (high anomaly density)
     * single  — a mostly-normal day with exactly ONE real incident
     * drift   — slow degradation (memory leak / rising drop-rate / thermal creep)
                 that trends over hours and finally breaches — exercises the
                 metric_slope_short/long drift features.

Normal log taxonomy (research-grounded, vendor-neutral)
-------------------------------------------------------
routing   OSPF/BGP/RIB: adjacency up, LSA/keepalive, SPF, route add/withdraw churn
switching STP topology stable, MAC learn/age, ARP refresh, VLAN converge, LACP up
interface link state, counters, optics in-range, SFP insert (maintenance)
environment temperature/fan/PSU/voltage OK, ASIC health, ECC scrub clean
system    NTP sync, SNMP poll, AAA login/logout ok, config save, DHCP lease, SSH
redundancy peer sync verified, heartbeat, checkpoint, state SYNCED
health    CPU/mem within bounds, threshold monitors OK, watchdog, queue/buffer ok

Usage
-----
    python scripts/gen_network_logs.py --train-out data/raw/train_clean_june2026 \
        --test-out data/raw/test_july2026
    python scripts/gen_network_logs.py --only train      # just the 30 clean days
    python scripts/gen_network_logs.py --only test       # just the test scenarios

Then load, e.g.:
    python pipeline.py --dry-run --input-mode synthetic \
        --log-file data/raw/train_clean_june2026
"""
from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timedelta

HOST = "network_device"
SEED = 20260601

# ---------------------------------------------------------------------------
# Small formatting helpers
# ---------------------------------------------------------------------------

def _iso_z(dt: datetime) -> str:
    """[2026-06-01T02:57:26.935Z] form used in Section 1."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _iso_tz(dt: datetime) -> str:
    """2026-06-01T00:03:21.216+00:00 form used in Sections 2 & 5."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}+00:00"


def _hms(dt: datetime) -> str:
    """[HH:MM:SS.ms] form used in Sections 3 & 4."""
    return dt.strftime("%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def _ms_epoch(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


# ---------------------------------------------------------------------------
# Randomised field helpers
# ---------------------------------------------------------------------------

def _ip(rng):
    return f"10.{rng.randint(0, 254)}.{rng.randint(0, 254)}.{rng.randint(1, 254)}"


def _net(rng):
    return f"10.{rng.randint(0, 254)}.{rng.randint(0, 254)}.0/{rng.choice([24, 25, 23, 22])}"


def _mac(rng):
    return ":".join(f"{rng.randint(0, 255):02x}" for _ in range(6))


def _vlan(rng):
    return rng.randint(2, 4090)


def _port(rng):
    return f"1/1/{rng.randint(1, 48)}"


# ---------------------------------------------------------------------------
# Normal message catalog — each entry returns (component, event_name, kv-text)
# for Section 1, or a plain message string for Sections 2/3/5.
# ---------------------------------------------------------------------------

def normal_section1(rng):
    """Return (event_name, component, fields_dict) for a routine structured event."""
    choices = [
        ("ROUTING_TABLE_STABLE", "ROUTING", {"routes": rng.randint(12000, 16000), "changes": 0}),
        ("PEER_SYNC_VERIFIED", "REDUNDANCY", {"synced_objects": rng.randint(2000, 3200), "sync_state": "SYNCED"}),
        ("OSPF_ADJACENCY_UP", "ROUTING", {"neighbor": _ip(rng), "area": rng.randint(0, 4), "state": "FULL"}),
        ("BGP_KEEPALIVE", "ROUTING", {"peer": _ip(rng), "hold_time": 180, "state": "ESTABLISHED"}),
        ("STP_TOPOLOGY_STABLE", "STP", {"root_changes": 0, "vlan": _vlan(rng)}),
        ("LACP_BOND_STABLE", "INTERFACE", {"bond": f"po{rng.randint(1, 8)}", "members_up": rng.randint(2, 4)}),
        ("MAC_TABLE_SYNC", "SWITCHING", {"entries": rng.randint(8000, 24000), "moves": rng.randint(0, 3)}),
        ("ENV_HEALTH_OK", "ENVIRONMENT", {"temp_c": rng.randint(30, 52), "fan_rpm": rng.randint(4200, 7200), "psu": "OK"}),
        ("ECC_SCRUB_CLEAN", "ASIC", {"region": f"mem{rng.randint(0, 7)}", "errors": 0}),
        ("CONFIG_CHECKPOINT", "MGMT", {"revision": rng.randint(9000, 9999), "result": "OK"}),
        ("REDUNDANCY_HEALTHY", "REDUNDANCY", {"active": "primary", "standby": "ready"}),
        ("INTERFACE_OPTICS_OK", "INTERFACE", {"port": _port(rng), "rx_dbm": round(rng.uniform(-6.5, -2.0), 2), "tx_dbm": round(rng.uniform(-3.0, 1.0), 2)}),
    ]
    return rng.choice(choices)


def normal_syslog(rng):
    """Return (tag, message) for a routine Section-2/5 syslog line. INFO content."""
    choices = [
        ("ospf_daemon", f"OSPF adjacency established on vlan{_vlan(rng)}"),
        ("rib_manager", f"Route {_net(rng)} added to RIB via OSPF"),
        ("rib_manager", f"Route {_net(rng)} withdrawn, RIB updated"),
        ("rib_manager", "Continuous monitoring active"),
        ("bgp_daemon", f"BGP peer {_ip(rng)} keepalive received, session ESTABLISHED"),
        ("arp_watcher", f"ARP entry {_ip(rng)} refreshed via {_mac(rng)}"),
        ("topology_monitor", f"ARP entry {_ip(rng)} refreshed via {_mac(rng)}"),
        ("mac_learn", f"MAC {_mac(rng)} learned on {_port(rng)} vlan{_vlan(rng)}"),
        ("peer_heartbeat", f"VLAN {_vlan(rng)} forwarding table converged"),
        ("baseline_check", "Network topology stable"),
        ("ntp_client", f"Clock synchronized to stratum {rng.randint(1, 3)}, offset {round(rng.uniform(-4, 4), 2)}ms"),
        ("ntp_client", f"Swap usage {rng.randint(5, 18)}% below threshold, no action required"),
        ("snmp_agent", f"SNMP walk completed from {_ip(rng)}, OID ifTable"),
        ("rib_manager", f"SNMP walk completed from {_ip(rng)}, OID ifTable"),
        ("arp_watcher", f"Threshold monitor: CPU {rng.randint(55, 78)}% within normal bounds"),
        ("health_monitor", f"Threshold monitor: memory {rng.randint(40, 72)}% within normal bounds"),
        ("auth_manager", f"Admin session opened for user netops from {_ip(rng)} (TACACS+ ok)"),
        ("auth_manager", f"Admin session closed for user netops from {_ip(rng)}"),
        ("ospf_daemon", f"Configuration saved successfully, revision {rng.randint(9000, 9999)}"),
        ("dhcp_relay", f"DHCP lease granted {_ip(rng)} to {_mac(rng)}, lease 86400s"),
        ("dhcp_relay", f"DHCP lease renewed {_ip(rng)}"),
        ("ssh_server", f"SSH session established from {_ip(rng)} for user monitor"),
        ("lldp_agent", f"LLDP neighbor discovered on {_port(rng)}"),
        ("monitoring_active", f"OSPF adjacency established on vlan{_vlan(rng)}"),
        ("interface_mgr", f"Interface {_port(rng)} counters polled, no errors"),
        ("env_monitor", f"Temperature {rng.randint(30, 52)}C nominal, fans steady"),
    ]
    return rng.choice(choices)


def normal_debug(rng):
    """Return (codefile, func, message) for a routine Section-3 trace. INFO content."""
    choices = [
        ("routing.c", "rib_insert", "route processed"),
        ("routing.c", "rib_delete", "stale route reclaimed"),
        ("interface.c", "counter_poll", "counters sampled"),
        ("asic_ecc.c", "ecc_scrub", "scrub pass clean"),
        ("security.c", "auth_check", "session validated"),
        ("health.c", "state_handler", "health tick ok"),
        ("spanning_tree.c", "stp_tick", "topology unchanged"),
        ("redundancy.c", "sync_checkpoint", "checkpoint committed"),
        ("ntp.c", "clock_discipline", "offset within bounds"),
    ]
    cf, fn, msg = rng.choice(choices)
    return cf, f"{cf[:-2]}:{rng.randint(100, 999)}", fn, msg


# ---------------------------------------------------------------------------
# Diurnal model
# ---------------------------------------------------------------------------

def diurnal_weight(hour: int, weekend: bool) -> float:
    """Relative activity multiplier across the day (peak in business hours)."""
    # base sinusoid peaking ~13:00
    import math
    base = 0.45 + 0.55 * max(0.0, math.sin((hour - 6) / 24 * 2 * math.pi))
    if weekend:
        base *= 0.6
    return base


def metric_baselines(hour: int, weekend: bool, rng):
    """Normal cpu/mem/util/temp/drop for a Section-4 sample at this hour."""
    w = diurnal_weight(hour, weekend)
    cpu = max(3.0, min(85.0, rng.gauss(18 + 45 * w, 8)))
    mem = max(20.0, min(80.0, rng.gauss(45 + 12 * w, 7)))
    util = max(2.0, min(92.0, rng.gauss(20 + 55 * w, 10)))
    temp = max(28.0, min(58.0, rng.gauss(36 + 10 * w, 3)))
    drop = max(0.0, rng.gauss(0.02 * w, 0.02))  # ~0, tiny
    return cpu, mem, util, temp, drop


# ---------------------------------------------------------------------------
# Day builder
# ---------------------------------------------------------------------------

class DayLog:
    """Accumulates section rows for one day, then renders the 7-section file."""

    def __init__(self, date: datetime, scenario_num: int, title: str, severity: str,
                 desc: str, failure_type: str, impact: str):
        self.date = date
        self.scenario_num = scenario_num
        self.title = title
        self.severity = severity
        self.desc = desc
        self.failure_type = failure_type
        self.impact = impact
        self.s1: list[tuple[datetime, str]] = []   # (ts, rendered block)
        self.s2: list[tuple[datetime, str]] = []
        self.s3: list[tuple[datetime, str]] = []
        self.s4: list[tuple[datetime, str]] = []
        self.s5: list[tuple[datetime, str]] = []
        self._eid = 20000
        # Section 7 / 6 fields
        self.training_label = "NORMAL_OPERATION"
        self.failure_mode = "none"
        self.root_cause = "No failure - device operating within all normal parameters"
        self.affected = []
        self.signals = ["ALL_CLEAR", "NORMAL_OPERATION"]
        self.root_chain = "ALL_CLEAR -> NORMAL_OPERATION"

    def eid(self):
        self._eid += 1
        return self._eid

    def add_event(self, ts, event_name, component, severity, fields: dict):
        kv = " | ".join(f"{k}={v}" for k, v in fields.items())
        block = (
            f"[{_iso_z(ts)}] EVENT: {event_name} | severity={severity} | component={component} |\n"
            f"   event_id={self.eid()} | {kv} | timestamp_ms={_ms_epoch(ts)}"
        )
        self.s1.append((ts, block))

    def add_syslog(self, ts, tag, message, section=2):
        line = f"{_iso_tz(ts)} {HOST} {tag}: {tag}: {message}"
        (self.s2 if section == 2 else self.s5).append((ts, line))

    def add_debug(self, ts, codeloc, func, message):
        self.s3.append((ts, f"[{_hms(ts)}] {codeloc} | {func}: {message}"))

    def add_metric(self, ts, entity, kvs: dict):
        body = " ".join(f"{k}={v}" for k, v in kvs.items())
        self.s4.append((ts, f"[{_hms(ts)}] {entity}: {body}"))

    def render(self) -> str:
        sev_dur = self.date.strftime("%Y-%m-%d")
        nxt = (self.date + timedelta(days=1)).strftime("%Y-%m-%d")
        out = []
        out.append(f"# SYNTHETIC LOG DATASET - Scenario {self.scenario_num:02d}: {self.title} ({self.severity})")
        out.append("# WARNING: SYNTHETIC, VENDOR-NEUTRAL dataset for training purposes ONLY")
        out.append(f"# Scenario: {self.desc}")
        out.append(f"# Failure Type: {self.failure_type}")
        out.append(f"# Severity: {self.severity}")
        out.append(f"# Duration: {sev_dur} 00:00:00 to {nxt} 00:00:00 (86400 seconds)")
        out.append(f"# Impact: {self.impact}")
        out.append("")
        out.append("")

        out.append("## SECTION 1: STRUCTURED EVENTS")
        for _, block in sorted(self.s1, key=lambda x: x[0]):
            out.append(block)
            out.append("")
        out.append("## SECTION 2: SYSLOG ENTRIES")
        for _, line in sorted(self.s2, key=lambda x: x[0]):
            out.append(line)
        out.append("")
        out.append("## SECTION 3: DEBUG/TRACE LOGS")
        for _, line in sorted(self.s3, key=lambda x: x[0]):
            out.append(line)
        out.append("")
        out.append("## SECTION 4: PERFORMANCE METRICS")
        for _, line in sorted(self.s4, key=lambda x: x[0]):
            out.append(line)
        out.append("")
        out.append("## SECTION 5: ADDITIONAL CONTEXTUAL LOGS")
        for _, line in sorted(self.s5, key=lambda x: x[0]):
            out.append(line)
        out.append("")
        out.append("## SECTION 6: CORRELATION ANALYSIS")
        out.append(f"Root cause chain: {self.root_chain}")
        out.append(f"Primary affected components: {', '.join(self.affected)}")
        out.append("")
        out.append("## SECTION 7: TRAINING METADATA")
        out.append(f"training_label: {self.training_label}")
        out.append(f"failure_mode: {self.failure_mode}")
        out.append(f"root_cause: {self.root_cause}")
        out.append(f"file_severity: {self.severity}")
        out.append(f"affected_components: [{', '.join(self.affected)}]")
        out.append(f"correlation_signals: [{', '.join(self.signals)}]")
        out.append("")
        return "\n".join(out)


def _rand_ts(date: datetime, hour: int, rng) -> datetime:
    return date + timedelta(hours=hour, minutes=rng.randint(0, 59),
                            seconds=rng.randint(0, 59), milliseconds=rng.randint(0, 999))


def populate_normal(day: DayLog, rng, *, weekend: bool, maintenance: bool,
                    syslog_n=4200, s1_n=160, s3_n=130, s5_n=55):
    """Fill a DayLog with a full day of normal activity, diurnally weighted."""
    # hour weights for sampling event times
    hours = list(range(24))
    weights = [diurnal_weight(h, weekend) for h in hours]

    def sample_hour():
        return rng.choices(hours, weights=weights, k=1)[0]

    # Section 2 syslog — the operational bulk
    for _ in range(syslog_n):
        ts = _rand_ts(day.date, sample_hour(), rng)
        tag, msg = normal_syslog(rng)
        day.add_syslog(ts, tag, msg, section=2)

    # Section 1 structured events
    for _ in range(s1_n):
        ts = _rand_ts(day.date, sample_hour(), rng)
        name, comp, fields = normal_section1(rng)
        day.add_event(ts, name, comp, "INFO", fields)

    # Section 3 debug traces
    for _ in range(s3_n):
        ts = _rand_ts(day.date, sample_hour(), rng)
        cf, loc, fn, msg = normal_debug(rng)
        day.add_debug(ts, loc, fn, msg)

    # Section 5 contextual
    for _ in range(s5_n):
        ts = _rand_ts(day.date, sample_hour(), rng)
        tag, msg = normal_syslog(rng)
        day.add_syslog(ts, tag, msg, section=5)

    # Section 4 metrics — every 15 minutes
    for q in range(96):
        ts = day.date + timedelta(minutes=15 * q, seconds=rng.randint(0, 59))
        cpu, mem, util, temp, drop = metric_baselines(ts.hour, weekend, rng)
        entity = rng.choice(["ControlPlane", "SystemHealth", "Baseline", "DataPlane"])
        day.add_metric(ts, entity, {
            "cpu_pct": round(cpu, 2), "mem_pct": round(mem, 2), "link_util_pct": round(util, 2),
        })
        day.add_metric(ts, "Environment", {
            "temperature_c": round(temp, 2), "drop_rate": round(drop, 4),
            "buffer_pct": round(max(2, rng.gauss(20 + 25 * diurnal_weight(ts.hour, weekend), 8)), 2),
        })

    # Periodic jobs — NTP every 4h, nightly config backup ~02:00
    for h in range(0, 24, 4):
        ts = day.date + timedelta(hours=h, minutes=rng.randint(0, 5))
        day.add_syslog(ts, "ntp_client", f"Clock synchronized to stratum {rng.randint(1, 3)}, "
                       f"offset {round(rng.uniform(-3, 3), 2)}ms", section=2)
    bts = day.date + timedelta(hours=2, minutes=rng.randint(0, 30))
    day.add_syslog(bts, "config_agent", f"Nightly configuration backup completed, "
                   f"revision {rng.randint(9000, 9999)}", section=2)

    # Maintenance window — still NORMAL, just busier: planned config pushes, admin
    # sessions, a couple of planned interface bounces (admin-down/up). INFO only.
    if maintenance:
        start = day.date + timedelta(hours=rng.choice([1, 2, 23]))
        day.failure_type = "Normal Operation (planned maintenance window)"
        day.impact = "None - planned maintenance, no service impact"
        day.signals = ["MAINTENANCE_WINDOW", "PLANNED_CHANGE", "ALL_CLEAR"]
        for i in range(rng.randint(25, 45)):
            ts = start + timedelta(minutes=rng.randint(0, 90), seconds=rng.randint(0, 59))
            kind = rng.random()
            if kind < 0.4:
                day.add_syslog(ts, "config_agent",
                               f"Configuration committed by netops, revision {rng.randint(9000, 9999)} (planned)", section=2)
            elif kind < 0.7:
                port = _port(rng)
                day.add_syslog(ts, "interface_mgr",
                               f"Interface {port} administratively down for planned maintenance", section=2)
                day.add_syslog(ts + timedelta(seconds=rng.randint(20, 120)), "interface_mgr",
                               f"Interface {port} administratively up, link restored", section=2)
            else:
                day.add_event(ts, "MAINTENANCE_CHECKPOINT", "MGMT", "INFO",
                              {"window": "planned", "operator": "netops", "result": "OK"})


# ---------------------------------------------------------------------------
# Incident injectors (test data only)
# ---------------------------------------------------------------------------

def inject_incident(day: DayLog, rng, kind: str, start: datetime):
    """Inject a correlated multi-line incident cascade starting at `start`.

    Returns the (label, affected_components, signals) describing it. Lines carry
    ERROR/CRITICAL severity (explicit in Section 1, content-inferred in 2/3).
    """
    def burst(ts_offsets, emit):
        for off in ts_offsets:
            emit(start + timedelta(seconds=off))

    if kind == "interface_flap":
        port = _port(rng)
        day.add_event(start, "INTERFACE_LINK_FLAP", "INTERFACE", "ERROR",
                      {"port": port, "flap_count": rng.randint(8, 40), "state": "DOWN"})
        burst([2, 5, 9, 14, 20, 27, 35, 44, 60, 78],
              lambda t: day.add_syslog(t, "interface_mgr",
                  f"Interface {port} link down, carrier lost", section=2))
        burst([3, 12, 25, 40, 55, 72],
              lambda t: day.add_debug(t, "interface.c:681", "link_fsm",
                  "ERROR carrier transition flapping, debounce exceeded"))
        day.add_event(start + timedelta(seconds=90), "OSPF_ADJACENCY_LOSS", "ROUTING", "ERROR",
                      {"neighbor": _ip(rng), "reason": "interface_down", "state": "DOWN"})
        return "INTERFACE_FLAP", ["INTERFACE", "ROUTING"], ["LINK_FLAP", "OSPF_ADJ_LOSS", "CARRIER_LOST"]

    if kind == "ospf_storm":
        burst([0, 4, 9, 15, 22, 30, 39, 49, 60, 72, 85, 99],
              lambda t: day.add_syslog(t, "ospf_daemon",
                  f"OSPF neighbor {_ip(rng)} down, adjacency lost (dead timer expired)", section=2))
        day.add_event(start + timedelta(seconds=20), "OSPF_RECONVERGENCE", "ROUTING", "CRITICAL",
                      {"lost_adjacencies": rng.randint(4, 12), "spf_runs": rng.randint(20, 60), "state": "RECONVERGING"})
        burst([10, 25, 45, 70, 95],
              lambda t: day.add_debug(t, "routing.c:452", "spf_compute",
                  "CRITICAL SPF recomputation storm, routing table unstable"))
        return "OSPF_RECONVERGENCE_STORM", ["ROUTING"], ["OSPF_ADJ_LOSS", "SPF_STORM", "ROUTE_FLAP"]

    if kind == "bgp_drop":
        peer = _ip(rng)
        day.add_event(start, "BGP_SESSION_DROP", "ROUTING", "ERROR",
                      {"peer": peer, "reason": "hold_timer_expired", "state": "IDLE"})
        burst([3, 8, 16, 27, 40, 56, 75],
              lambda t: day.add_syslog(t, "bgp_daemon",
                  f"BGP peer {peer} session reset, {rng.randint(2000, 9000)} prefixes withdrawn", section=2))
        return "BGP_SESSION_DROP", ["ROUTING"], ["BGP_DROP", "PREFIX_WITHDRAW"]

    if kind == "lacp_degraded":
        bond = f"po{rng.randint(1, 8)}"
        day.add_event(start, "LACP_BOND_DEGRADED", "INTERFACE", "ERROR",
                      {"bond": bond, "members_down": rng.randint(1, 3), "state": "DEGRADED"})
        burst([4, 11, 21, 34, 50],
              lambda t: day.add_syslog(t, "lacp_agent",
                  f"LACP member {_port(rng)} on {bond} removed, partner timeout", section=2))
        return "LACP_DEGRADED", ["INTERFACE"], ["LACP_DEGRADED", "MEMBER_DOWN"]

    if kind == "asic_ecc":
        day.add_event(start, "ASIC_ECC_ERRORS", "ASIC", "CRITICAL",
                      {"region": f"mem{rng.randint(0,7)}", "uncorrectable": rng.randint(2, 9), "state": "DEGRADED"})
        burst([2, 6, 13, 22, 33, 47, 64],
              lambda t: day.add_debug(t, "asic_ecc.c:452", "ecc_handler",
                  "CRITICAL uncorrectable ECC error, memory region degraded"))
        day.add_event(start + timedelta(seconds=80), "FORWARDING_DEGRADED", "ASIC", "ERROR",
                      {"dropped_pps": rng.randint(1000, 50000), "state": "DEGRADED"})
        return "ASIC_ECC_FAILURE", ["ASIC", "DATAPLANE"], ["ECC_UNCORRECTABLE", "FORWARDING_DEGRADED"]

    if kind == "psu_fault":
        day.add_event(start, "PSU_FAULT", "ENVIRONMENT", "CRITICAL",
                      {"psu": rng.randint(1, 2), "voltage": 0, "state": "FAILED"})
        burst([3, 9, 18, 30],
              lambda t: day.add_syslog(t, "env_monitor",
                  f"Power supply {rng.randint(1,2)} failure detected, redundancy lost", section=2))
        return "PSU_FAILURE", ["ENVIRONMENT"], ["PSU_FAILED", "REDUNDANCY_LOST"]

    if kind == "oom":
        # memory pressure → OOM kill cascade (sparse-ish severe burst)
        day.add_event(start, "MEMORY_PRESSURE", "SYSTEM", "ERROR",
                      {"mem_pct": rng.randint(92, 97), "trend": "rising", "state": "PRESSURE"})
        burst([15, 45, 95, 160, 240, 330, 430, 540],
              lambda t: day.add_debug(t, "memory.c:128", "oom_handler",
                  "CRITICAL out of memory, sent SIGKILL to reclaim pages"))
        day.add_event(start + timedelta(seconds=560), "PROCESS_KILLED", "SYSTEM", "CRITICAL",
                      {"process": "route_daemon", "signal": "SIGKILL", "state": "RESTARTING"})
        return "MEMORY_OOM_CASCADE", ["SYSTEM", "ROUTING"], ["MEMORY_PRESSURE", "OOM_KILL", "PROCESS_RESTART"]

    raise ValueError(kind)


def inject_drift(day: DayLog, rng, kind: str):
    """Inject a slow degradation across the whole day into Section-4 metrics +
    periodic escalating log lines. Exercises metric_slope_short/long."""
    if kind == "memleak":
        # mem_pct ramps 48 -> 96 across the day; warnings begin ~16h, error ~21h
        for q in range(96):
            ts = day.date + timedelta(minutes=15 * q, seconds=rng.randint(0, 59))
            frac = q / 95.0
            mem = min(97.0, 48 + 48 * frac + rng.gauss(0, 1.2))
            # dedicated entity so the monotonic ramp is its own metric series and
            # the slope feature isn't diluted by the flat backdrop SystemHealth.
            day.add_metric(ts, "MemoryPool", {"mem_pct": round(mem, 2),
                           "cpu_pct": round(metric_baselines(ts.hour, False, rng)[0], 2)})
            if frac > 0.65 and q % 3 == 0:
                lvl = "WARNING" if frac < 0.85 else "ERROR"
                day.add_syslog(ts, "memory_monitor",
                    f"Heap growth detected, memory at {mem:.0f}% and rising "
                    f"({'threshold approaching' if lvl=='WARNING' else 'threshold exceeded'})", section=2)
        # terminal breach — the leak finally triggers a dense OOM-kill cascade
        brk = day.date + timedelta(hours=21, minutes=30)
        day.add_event(brk, "MEMORY_LEAK_SUSPECTED", "SYSTEM", "ERROR",
                      {"mem_pct": 95, "growth_rate_mb_h": rng.randint(80, 200), "trend": "monotonic_rising"})
        for off in [20, 55, 95, 140, 190, 245, 305]:
            day.add_debug(brk + timedelta(seconds=off), "memory.c:128", "oom_handler",
                          "CRITICAL out of memory, sent SIGKILL to reclaim pages")
        day.add_event(brk + timedelta(seconds=320), "PROCESS_KILLED", "SYSTEM", "CRITICAL",
                      {"process": "route_daemon", "signal": "SIGKILL", "state": "RESTARTING"})
        return "GRADUAL_MEMORY_LEAK", ["SYSTEM"], ["MEMORY_PRESSURE", "HEAP_GROWTH", "SLOW_DRIFT", "OOM_KILL"]

    if kind == "droprate":
        # drop_rate creeps 0 -> 8%; congestion warnings build through the day
        for q in range(96):
            ts = day.date + timedelta(minutes=15 * q, seconds=rng.randint(0, 59))
            frac = q / 95.0
            drop = max(0.0, 8.0 * (frac ** 2) + rng.gauss(0, 0.15))
            util = min(98.0, 40 + 55 * frac + rng.gauss(0, 4))
            day.add_metric(ts, "Uplink1", {"drop_rate": round(drop, 3),
                           "link_util_pct": round(util, 2), "queue_pct": round(min(99, 30 + 65 * frac), 2)})
            if frac > 0.55 and q % 4 == 0:
                day.add_syslog(ts, "qos_monitor",
                    f"Egress drop rate increasing, now {drop:.1f}% on uplink, queue depth growing", section=2)
        # terminal breach — congestion collapses into a dense drop/error cascade
        brk = day.date + timedelta(hours=22)
        day.add_event(brk, "CONGESTION_THRESHOLD_EXCEEDED", "DATAPLANE", "ERROR",
                      {"drop_rate_pct": 8, "trend": "rising", "state": "CONGESTED"})
        for off in [5, 12, 22, 35, 51, 70, 92, 118]:
            day.add_syslog(brk + timedelta(seconds=off), "qos_monitor",
                f"ERROR egress queue overflow, packets dropped on uplink, tail-drop active", section=2)
        day.add_event(brk + timedelta(seconds=130), "FORWARDING_DEGRADED", "DATAPLANE", "CRITICAL",
                      {"dropped_pps": rng.randint(20000, 90000), "state": "CONGESTED"})
        return "GRADUAL_DROPRATE_DRIFT", ["DATAPLANE", "QOS"], ["DROP_RATE_INCREASING", "CONGESTION", "SLOW_DRIFT", "TAIL_DROP"]

    if kind == "thermal":
        # temperature creeps 36 -> 74C; fan ramps; thermal warnings late
        for q in range(96):
            ts = day.date + timedelta(minutes=15 * q, seconds=rng.randint(0, 59))
            frac = q / 95.0
            temp = 36 + 38 * frac + rng.gauss(0, 0.8)
            fan = min(15000, 4500 + 9000 * frac)
            day.add_metric(ts, "InletSensor", {"temperature_c": round(temp, 2),
                           "fan_rpm": round(fan), "cpu_pct": round(metric_baselines(ts.hour, False, rng)[0], 2)})
            if frac > 0.7 and q % 3 == 0:
                lvl = "approaching" if frac < 0.88 else "exceeded"
                day.add_syslog(ts, "env_monitor",
                    f"Inlet temperature {temp:.0f}C, thermal threshold {lvl}, fans at max", section=2)
        # terminal breach — overheating forces a dense throttle/shutdown cascade
        brk = day.date + timedelta(hours=22, minutes=15)
        day.add_event(brk, "THERMAL_THRESHOLD_EXCEEDED", "ENVIRONMENT", "ERROR",
                      {"temperature_c": 74, "fan_rpm": 15000, "trend": "rising"})
        for off in [8, 20, 36, 56, 80, 108]:
            day.add_debug(brk + timedelta(seconds=off), "thermal.c:204", "thermal_guard",
                          "CRITICAL temperature threshold exceeded, throttling line cards")
        day.add_event(brk + timedelta(seconds=120), "THERMAL_SHUTDOWN_IMMINENT", "ENVIRONMENT", "CRITICAL",
                      {"temperature_c": 78, "action": "throttle", "state": "CRITICAL"})
        return "GRADUAL_THERMAL_CREEP", ["ENVIRONMENT"], ["TEMP_RISING", "FAN_RAMP", "SLOW_DRIFT", "THERMAL_THROTTLE"]

    raise ValueError(kind)


# ---------------------------------------------------------------------------
# Top-level generation
# ---------------------------------------------------------------------------

def gen_training(out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    rng = random.Random(SEED)
    maintenance_days = {4, 17}  # two planned-maintenance nights within the month
    for d in range(1, 31):
        date = datetime(2026, 6, d)
        weekend = date.weekday() >= 5
        maintenance = d in maintenance_days
        kind = "weekend" if weekend else "weekday"
        if maintenance:
            kind = "maintenance"
        title = f"Clean Baseline Day {d:02d} - {'Weekend Quiet' if weekend else 'Weekday Steady-State'}" \
                + (" (Maintenance Window)" if maintenance else "")
        day = DayLog(date, d, title, "INFO",
                     f"Normal {kind} operation, full healthy-behaviour variety, no incidents",
                     "Normal Operation", "None - zero incidents, zero alerts")
        # weekends quieter; maintenance busier
        sysn = 2600 if weekend else 4200
        populate_normal(day, rng, weekend=weekend, maintenance=maintenance, syslog_n=sysn)
        path = os.path.join(out_dir, f"daily_2026-06-{d:02d}.log")
        with open(path, "w") as fh:
            fh.write(day.render())
    print(f"[train] wrote 30 clean days -> {out_dir}")


def _make_test_day(date, num, title, severity, desc, ftype, impact, rng, weekend=False):
    day = DayLog(date, num, title, severity, desc, ftype, impact)
    # a realistic normal backdrop (lighter so incidents stand out)
    populate_normal(day, rng, weekend=weekend, maintenance=False,
                    syslog_n=2200, s1_n=110, s3_n=90, s5_n=40)
    return day


def gen_testing(out_dir: str):
    rng = random.Random(SEED + 7)
    base = datetime(2026, 7, 1)

    def finalize(day, label, affected, signals, sev):
        day.training_label = label
        day.failure_mode = label.lower()
        day.severity = sev
        day.affected = affected
        day.signals = signals
        day.root_cause = f"{label} - see correlated cascade"
        day.root_chain = " -> ".join(signals)

    # ---- DENSE: many incidents in one day (2 variants) ----
    dense_dir = os.path.join(out_dir, "dense")
    os.makedirs(dense_dir, exist_ok=True)
    dense_kinds = ["interface_flap", "ospf_storm", "bgp_drop", "lacp_degraded",
                   "asic_ecc", "psu_fault", "oom"]
    for v in range(2):
        date = base + timedelta(days=v)
        day = _make_test_day(date, 60 + v, f"Anomalous Day - Dense Incidents (variant {v+1})",
                             "CRITICAL", "High anomaly density: multiple correlated incident cascades",
                             "Severe Multi-Incident", "Multiple incidents across subsystems", rng)
        n_inc = rng.randint(5, 7)
        all_aff, all_sig = set(), []
        for i in range(n_inc):
            kind = dense_kinds[(v + i) % len(dense_kinds)]
            start = date + timedelta(hours=rng.randint(1, 22), minutes=rng.randint(0, 59))
            _lbl, aff, sig = inject_incident(day, rng, kind, start)
            all_aff.update(aff); all_sig += sig
        finalize(day, "DENSE_MULTI_INCIDENT", sorted(all_aff), list(dict.fromkeys(all_sig)), "CRITICAL")
        with open(os.path.join(dense_dir, f"dense_anomaly_{v+1}.log"), "w") as fh:
            fh.write(day.render())

    # ---- SINGLE: exactly one incident on an otherwise-normal day (2 variants) ----
    single_dir = os.path.join(out_dir, "single")
    os.makedirs(single_dir, exist_ok=True)
    single_kinds = ["oom", "ospf_storm"]
    for v, kind in enumerate(single_kinds):
        date = base + timedelta(days=2 + v)
        day = _make_test_day(date, 70 + v, f"Single-Incident Day (variant {v+1})",
                             "ERROR", "Mostly-normal day with exactly ONE real incident cascade",
                             "Single Incident", "One localized incident, otherwise healthy", rng)
        start = date + timedelta(hours=rng.randint(9, 16), minutes=rng.randint(0, 59))
        lbl, aff, sig = inject_incident(day, rng, kind, start)
        finalize(day, f"SINGLE_{lbl}", aff, sig, "CRITICAL" if kind in ("oom",) else "ERROR")
        with open(os.path.join(single_dir, f"single_incident_{v+1}.log"), "w") as fh:
            fh.write(day.render())

    # ---- DRIFT: gradual degradation (3 variants: mem leak / drop rate / thermal) ----
    drift_dir = os.path.join(out_dir, "drift")
    os.makedirs(drift_dir, exist_ok=True)
    for v, kind in enumerate(["memleak", "droprate", "thermal"]):
        date = base + timedelta(days=4 + v)
        day = DayLog(date, 80 + v, f"Gradual Drift Day - {kind}",
                     "ERROR", f"Slow {kind} degradation trending over hours until threshold breach",
                     "Gradual Drift", "Progressive degradation, late-day threshold breach")
        # normal backdrop WITHOUT the default Section-4 metric loop overwriting the drift:
        populate_normal(day, rng, weekend=False, maintenance=False,
                        syslog_n=2200, s1_n=110, s3_n=90, s5_n=40)
        lbl, aff, sig = inject_drift(day, rng, kind)
        finalize(day, lbl, aff, sig, "ERROR")
        with open(os.path.join(drift_dir, f"drift_{kind}.log"), "w") as fh:
            fh.write(day.render())

    print(f"[test] wrote dense(2) single(2) drift(3) -> {out_dir}/{{dense,single,drift}}")


def main():
    ap = argparse.ArgumentParser(description="Generate 7-section synthetic network logs.")
    ap.add_argument("--train-out", default="data/raw/train_clean_june2026")
    ap.add_argument("--test-out", default="data/raw/test_july2026")
    ap.add_argument("--only", choices=["train", "test", "both"], default="both")
    args = ap.parse_args()
    if args.only in ("train", "both"):
        gen_training(args.train_out)
    if args.only in ("test", "both"):
        gen_testing(args.test_out)


if __name__ == "__main__":
    main()
