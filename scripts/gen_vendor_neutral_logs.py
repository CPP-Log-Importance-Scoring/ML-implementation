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
    # ── Scenario 7: Thermal Shutdown ────────────────────────────────────────
    {
        "key": "thermal_shutdown", "title": "Thermal Shutdown (ASIC Overheat)",
        "severity": "CRITICAL", "failure_type": "Hardware / Thermal Protection",
        "root_cause": "ASIC temperature exceeded critical threshold triggering emergency shutdown",
        "impact": "Device shutdown, full traffic loss, manual recovery required",
        "subsys": "THERMAL", "daemon": "thermal_monitor", "debug_file": "thermal.c",
        "duration_s": 360, "training_label": "CRITICAL_THERMAL_FAILURE", "failure_mode": "thermal_shutdown",
        "events": [
            ("ASIC_TEMP_WARNING", "WARN", {"asic_temp_c": "72", "threshold_c": "75"}),
            ("ASIC_TEMP_HIGH", "HIGH", {"asic_temp_c": "79", "threshold_c": "75"}),
            ("FAN_SPEED_MAXED", "HIGH", {"fan_pct": "100", "asic_temp_c": "83"}),
            ("ASIC_TEMP_CRITICAL", "CRITICAL", {"asic_temp_c": "91", "threshold_c": "85"}),
            ("EMERGENCY_SHUTDOWN_INITIATED", "CRITICAL", {"reason": "thermal_protection"}),
        ],
        "cascade": [
            ("thermal_monitor", "ASIC temperature 72C warning threshold crossed"),
            ("thermal_monitor", "Fan speed increased to 100% - thermal mitigation active"),
            ("thermal_monitor", "CRITICAL - ASIC temperature 91C exceeded shutdown threshold 85C"),
            ("system_logger", "CRITICAL - Emergency thermal shutdown initiated"),
            ("system_logger", "Device powering down - thermal protection engaged"),
        ],
        "metrics": ("Thermal", ["asic_temp_c", "inlet_temp_c", "fan_speed_pct"]),
        "signals": ["ASIC_TEMP_HIGH", "FAN_SPEED_MAXED", "THERMAL_SHUTDOWN"],
        "affected": ["thermal_monitor", "system_logger", "process_monitor"],
    },
    # ── Scenario 8: Flood / DDoS Traffic Attack ─────────────────────────────
    {
        "key": "ddos_flood_attack", "title": "DDoS Flood Attack (Control Plane Saturation)",
        "severity": "CRITICAL", "failure_type": "Security / Traffic Flooding",
        "root_cause": "Inbound packet flood saturating control-plane CPU and exhausting ACL resources",
        "impact": "Control plane unresponsive, management access lost, ACL table overflow",
        "subsys": "ACL", "daemon": "access_control_daemon", "debug_file": "acl.c",
        "duration_s": 480, "training_label": "CRITICAL_SECURITY_FLOOD", "failure_mode": "ddos_flood",
        "events": [
            ("INBOUND_PPS_SPIKE", "HIGH", {"pps": "8200000", "interface": "uplink_1"}),
            ("CONTROL_PLANE_OVERLOAD", "CRITICAL", {"cpu_pct": "99", "queue_drop": "true"}),
            ("ACL_TABLE_OVERFLOW", "CRITICAL", {"used": "16382", "capacity": "16384"}),
            ("MGMT_ACCESS_DEGRADED", "CRITICAL", {"ssh_timeout_ms": "30000"}),
            ("FLOOD_MITIGATED", "INFO", {"action": "rate_limit_applied", "interface": "uplink_1"}),
        ],
        "cascade": [
            ("statistics_collector", "Inbound PPS 8.2 Mpps on uplink_1 - far exceeds baseline"),
            ("access_control_daemon", "CRITICAL - ACL rule evaluation backlog, dropping control frames"),
            ("access_control_daemon", "CRITICAL - ACL TCAM table 99% full - entries dropping"),
            ("monitoring_daemon", "CRITICAL - Management session timeout due to CPU saturation"),
            ("access_control_daemon", "Rate limiter applied on uplink_1 - flood mitigated"),
        ],
        "metrics": ("Security", ["inbound_pps", "acl_table_util_pct", "control_cpu_pct"]),
        "signals": ["INBOUND_PPS_SPIKE", "CONTROL_PLANE_OVERLOAD", "ACL_TABLE_OVERFLOW"],
        "affected": ["access_control_daemon", "statistics_collector", "monitoring_daemon"],
    },
    # ── Scenario 9: NTP Desynchronization ───────────────────────────────────
    {
        "key": "ntp_desync", "title": "NTP Desynchronization (Clock Drift)",
        "severity": "HIGH", "failure_type": "Time Synchronization Failure",
        "root_cause": "NTP server unreachable causing device clock to drift beyond acceptable skew",
        "impact": "Log timestamp corruption, authentication failures, certificate validation errors",
        "subsys": "NTP", "daemon": "ntp_daemon", "debug_file": "ntp.c",
        "duration_s": 900, "training_label": "HIGH_NTP_DESYNC", "failure_mode": "clock_drift",
        "events": [
            ("NTP_SERVER_UNREACHABLE", "WARN", {"server": "ntp.primary", "stratum": "N/A"}),
            ("CLOCK_DRIFT_WARNING", "HIGH", {"drift_ms": "450", "threshold_ms": "200"}),
            ("AUTH_FAILURE_SPIKE", "HIGH", {"failures": "37", "interval_s": "60"}),
            ("CLOCK_DRIFT_CRITICAL", "CRITICAL", {"drift_ms": "3200", "threshold_ms": "1000"}),
            ("NTP_RESYNC_COMPLETE", "INFO", {"server": "ntp.secondary", "drift_ms": "12"}),
        ],
        "cascade": [
            ("ntp_daemon", "Primary NTP server ntp.primary unreachable - stratum unknown"),
            ("ntp_daemon", "Clock drift 450ms exceeds 200ms warning threshold"),
            ("access_control_daemon", "Authentication failures increasing - possible clock skew"),
            ("ntp_daemon", "CRITICAL - Clock drift 3200ms - system time unreliable"),
            ("ntp_daemon", "Synchronized to secondary NTP server - drift corrected"),
        ],
        "metrics": ("ClockSync", ["drift_ms", "stratum", "auth_failure_rate"]),
        "signals": ["NTP_SERVER_UNREACHABLE", "CLOCK_DRIFT_WARNING", "CLOCK_DRIFT_CRITICAL"],
        "affected": ["ntp_daemon", "access_control_daemon", "system_logger"],
    },
    # ── Scenario 10: Disk I/O Saturation ────────────────────────────────────
    {
        "key": "disk_io_saturation", "title": "Disk I/O Saturation (Log Volume Full)",
        "severity": "HIGH", "failure_type": "Storage / Disk I/O Bottleneck",
        "root_cause": "Log volume filling due to runaway debug logging causing I/O wait and write failures",
        "impact": "Syslog writes failing, telemetry export halted, config saves failing",
        "subsys": "STORAGE", "daemon": "disk_io_daemon", "debug_file": "disk.c",
        "duration_s": 540, "training_label": "HIGH_DISK_IO_FAILURE", "failure_mode": "disk_saturation",
        "events": [
            ("DISK_UTIL_WARNING", "WARN", {"disk_pct": "78", "volume": "/var/log"}),
            ("IOWAIT_SPIKE", "HIGH", {"iowait_pct": "42", "threshold_pct": "20"}),
            ("WRITE_LATENCY_HIGH", "HIGH", {"write_ms": "380", "normal_ms": "5"}),
            ("DISK_UTIL_CRITICAL", "CRITICAL", {"disk_pct": "96", "volume": "/var/log"}),
            ("LOG_ROTATION_FORCED", "INFO", {"freed_mb": "2048", "volume": "/var/log"}),
        ],
        "cascade": [
            ("disk_io_daemon", "Disk utilization /var/log at 78% - approaching threshold"),
            ("disk_io_daemon", "I/O wait 42% - write operations severely degraded"),
            ("telemetry_collector", "Telemetry export failed - disk write error"),
            ("disk_io_daemon", "CRITICAL - Disk /var/log at 96% - writes failing"),
            ("disk_io_daemon", "Forced log rotation complete - 2048 MB freed"),
        ],
        "metrics": ("DiskIO", ["disk_util_pct", "iowait_pct", "write_latency_ms"]),
        "signals": ["DISK_UTIL_WARNING", "IOWAIT_SPIKE", "WRITE_LATENCY_HIGH", "DISK_FULL"],
        "affected": ["disk_io_daemon", "telemetry_collector", "system_logger"],
    },
    # ── Scenario 11: STP Loop (Forwarding Loop) ──────────────────────────────
    {
        "key": "stp_loop", "title": "STP Loop (Forwarding Loop Detected)",
        "severity": "CRITICAL", "failure_type": "Layer 2 Forwarding Loop",
        "root_cause": "STP BPDU guard disabled on an edge port allowing a rogue switch to create a loop",
        "impact": "Broadcast storm, MAC table thrashing, near-100% link utilization, traffic blackout",
        "subsys": "STP", "daemon": "spanning_tree_daemon", "debug_file": "stp_state.c",
        "duration_s": 420, "training_label": "CRITICAL_L2_LOOP", "failure_mode": "stp_loop",
        "events": [
            ("BPDU_RECEIVED_EDGE_PORT", "WARN", {"port": "downlink_3", "bpdu_type": "config"}),
            ("TOPOLOGY_CHANGE_STORM", "CRITICAL", {"tc_count": "847", "interval_s": "10"}),
            ("MAC_TABLE_THRASH", "CRITICAL", {"flushes_per_s": "320"}),
            ("BROADCAST_STORM", "CRITICAL", {"rx_pps": "9800000", "util_pct": "99"}),
            ("LOOP_GUARD_ACTIVATED", "INFO", {"port": "downlink_3", "action": "err_disabled"}),
        ],
        "cascade": [
            ("spanning_tree_daemon", "Unexpected BPDU received on edge port downlink_3"),
            ("spanning_tree_daemon", "CRITICAL - Topology change storm: 847 TCs in 10 seconds"),
            ("mac_learning", "CRITICAL - MAC table flush rate 320/s - loop suspected"),
            ("statistics_collector", "CRITICAL - Broadcast storm: uplink_1 99% utilization"),
            ("spanning_tree_daemon", "Loop guard activated - port downlink_3 err-disabled"),
        ],
        "metrics": ("L2Loop", ["tc_rate_per_s", "mac_flush_rate", "broadcast_util_pct"]),
        "signals": ["BPDU_STORM", "MAC_TABLE_THRASH", "BROADCAST_STORM", "LOOP_GUARD"],
        "affected": ["spanning_tree_daemon", "mac_learning", "statistics_collector"],
    },
    # ── Scenario 12: VLAN Misconfiguration ───────────────────────────────────
    {
        "key": "vlan_misconfiguration", "title": "VLAN Misconfiguration (Trunk Mismatch)",
        "severity": "HIGH", "failure_type": "Configuration Error / VLAN Trunk Mismatch",
        "root_cause": "VLAN trunk allowed-list mismatch between peers causing silent traffic drop for 12 VLANs",
        "impact": "12 VLANs silently blackholed, 3400 hosts unreachable",
        "subsys": "VLAN", "daemon": "config_daemon", "debug_file": "vlan.c",
        "duration_s": 660, "training_label": "HIGH_VLAN_CONFIG_FAILURE", "failure_mode": "vlan_mismatch",
        "events": [
            ("VLAN_TRAFFIC_LOSS", "HIGH", {"vlan_ids": "100-111", "port": "uplink_1"}),
            ("TRUNK_MISMATCH_DETECTED", "HIGH", {"local_vlans": "1-200", "peer_vlans": "1-99,112-200"}),
            ("ARP_FAILURES_SPIKE", "HIGH", {"failed_arps": "3400", "interval_s": "30"}),
            ("CONFIG_INCONSISTENCY", "CRITICAL", {"mismatch_count": "12", "component": "vlan_trunk"}),
            ("VLAN_TRUNK_CORRECTED", "INFO", {"port": "uplink_1", "vlans_restored": "12"}),
        ],
        "cascade": [
            ("network_monitor", "Traffic loss detected on VLANs 100-111 via uplink_1"),
            ("config_daemon", "VLAN trunk allowed-list mismatch with peer on uplink_1"),
            ("arp_daemon", "ARP resolution failures: 3400 hosts unreachable"),
            ("config_daemon", "CRITICAL - Configuration inconsistency: 12 VLANs missing from trunk"),
            ("config_daemon", "Trunk config corrected - all 12 VLANs restored on uplink_1"),
        ],
        "metrics": ("VLAN", ["vlan_drop_count", "arp_failure_rate", "trunk_mismatch_vlans"]),
        "signals": ["VLAN_TRAFFIC_LOSS", "TRUNK_MISMATCH_DETECTED", "ARP_FAILURES_SPIKE"],
        "affected": ["config_daemon", "network_monitor", "arp_daemon"],
    },
    # ── Scenario 13: Config Rollback (Bad Config Push) ────────────────────────
    {
        "key": "config_rollback", "title": "Config Rollback (Bad Configuration Push)",
        "severity": "HIGH", "failure_type": "Configuration Management / Rollback",
        "root_cause": "Erroneous ACL config push blocked management traffic, triggering auto-rollback",
        "impact": "60s management outage during rollback, audit log generated",
        "subsys": "CONFIG", "daemon": "config_daemon", "debug_file": "config.c",
        "duration_s": 300, "training_label": "HIGH_CONFIG_ROLLBACK", "failure_mode": "config_rollback",
        "events": [
            ("CONFIG_CHANGE_APPLIED", "INFO", {"change_id": "CHG-4821", "lines_changed": "47"}),
            ("MGMT_ACCESS_LOST", "CRITICAL", {"protocol": "ssh", "reason": "acl_deny"}),
            ("ROLLBACK_TIMER_EXPIRED", "WARN", {"confirm_timeout_s": "60"}),
            ("AUTO_ROLLBACK_INITIATED", "CRITICAL", {"target_checkpoint": "pre-CHG-4821"}),
            ("CONFIG_ROLLBACK_COMPLETE", "INFO", {"mgmt_restored": "true", "duration_s": "8"}),
        ],
        "cascade": [
            ("config_daemon", "Configuration change CHG-4821 applied: 47 lines modified"),
            ("config_daemon", "CRITICAL - Management access lost via SSH post-config-change"),
            ("config_daemon", "Rollback confirmation timer expired - auto-rollback triggered"),
            ("config_daemon", "CRITICAL - Rolling back to checkpoint pre-CHG-4821"),
            ("config_daemon", "Rollback complete - management access restored"),
        ],
        "metrics": ("ConfigMgmt", ["pending_changes", "rollback_duration_s", "mgmt_reachability"]),
        "signals": ["MGMT_ACCESS_LOST", "ROLLBACK_TIMER_EXPIRED", "AUTO_ROLLBACK"],
        "affected": ["config_daemon", "access_control_daemon", "monitoring_daemon"],
    },
    # ── Scenario 14: Unplanned Cold Reboot ───────────────────────────────────
    {
        "key": "cold_reboot", "title": "Unplanned Cold Reboot (Watchdog Timeout)",
        "severity": "CRITICAL", "failure_type": "System Availability / Unplanned Reboot",
        "root_cause": "Kernel watchdog timeout due to deadlocked forwarding thread forcing hard reset",
        "impact": "Full device reboot, 4-minute traffic outage, peer re-convergence required",
        "subsys": "SYSTEM", "daemon": "process_monitor", "debug_file": "watchdog.c",
        "duration_s": 240, "training_label": "CRITICAL_UNPLANNED_REBOOT", "failure_mode": "cold_reboot",
        "events": [
            ("WATCHDOG_KICK_MISSED", "WARN", {"daemon": "forwarding_engine", "missed_count": "3"}),
            ("KERNEL_HANG_DETECTED", "CRITICAL", {"thread": "fwd_main", "stuck_ms": "8000"}),
            ("WATCHDOG_TIMEOUT", "CRITICAL", {"daemon": "forwarding_engine", "action": "hard_reset"}),
            ("SYSTEM_REBOOT_INITIATED", "CRITICAL", {"reason": "watchdog_timeout"}),
            ("SYSTEM_BOOT_COMPLETE", "INFO", {"uptime_s": "0", "services_up": "all"}),
        ],
        "cascade": [
            ("process_monitor", "Watchdog kick missed: forwarding_engine 3 consecutive misses"),
            ("process_monitor", "CRITICAL - Kernel thread fwd_main hung for 8000ms"),
            ("process_monitor", "CRITICAL - Watchdog timeout - initiating hard reset"),
            ("system_logger", "CRITICAL - Unplanned system reboot: watchdog_timeout"),
            ("system_logger", "System boot complete - all services operational"),
        ],
        "metrics": ("SystemAvail", ["watchdog_miss_count", "reboot_count", "uptime_s"]),
        "signals": ["WATCHDOG_KICK_MISSED", "KERNEL_HANG_DETECTED", "WATCHDOG_TIMEOUT", "REBOOT"],
        "affected": ["process_monitor", "forwarding_engine", "system_logger"],
    },
    # ── Scenario 15: STP Topology Change Storm ───────────────────────────────
    {
        "key": "stp_tc_storm", "title": "STP Topology Change Storm (Flapping Edge Ports)",
        "severity": "HIGH", "failure_type": "Layer 2 Instability / TC Storm",
        "root_cause": "Flapping edge ports generating continuous STP topology change notifications causing MAC flush loops",
        "impact": "Sustained MAC table churn, CPU 60%, intermittent packet loss",
        "subsys": "STP", "daemon": "spanning_tree_daemon", "debug_file": "stp_tc.c",
        "duration_s": 600, "training_label": "HIGH_STP_TC_STORM", "failure_mode": "tc_storm",
        "events": [
            ("EDGE_PORT_FLAPPING", "WARN", {"port": "downlink_4", "flap_count": "22", "window_s": "60"}),
            ("TC_RATE_HIGH", "HIGH", {"tc_per_min": "180", "normal_tc_per_min": "5"}),
            ("MAC_FLUSH_EXCESS", "HIGH", {"flushes_per_min": "180"}),
            ("CPU_ELEVATED_STP", "HIGH", {"cpu_pct": "62", "stp_share_pct": "45"}),
            ("EDGE_PORT_STABILIZED", "INFO", {"port": "downlink_4", "action": "portfast_enabled"}),
        ],
        "cascade": [
            ("spanning_tree_daemon", "Edge port downlink_4 flapping: 22 state changes in 60s"),
            ("spanning_tree_daemon", "TC rate 180/min far exceeds normal 5/min baseline"),
            ("mac_learning", "MAC table flush triggered 180 times per minute"),
            ("statistics_collector", "CPU 62% - STP TC processing consuming 45% CPU share"),
            ("spanning_tree_daemon", "PortFast enabled on downlink_4 - TC storm suppressed"),
        ],
        "metrics": ("STPStorm", ["tc_rate_per_min", "mac_flush_rate", "stp_cpu_pct"]),
        "signals": ["EDGE_PORT_FLAPPING", "TC_RATE_HIGH", "MAC_FLUSH_EXCESS", "CPU_ELEVATED"],
        "affected": ["spanning_tree_daemon", "mac_learning", "statistics_collector"],
    },
    # ── Scenario 16: Heartbeat / Peer Keepalive Timeout ──────────────────────
    {
        "key": "heartbeat_timeout", "title": "Heartbeat Timeout (Peer Keepalive Loss)",
        "severity": "HIGH", "failure_type": "High Availability / Peer Reachability Failure",
        "root_cause": "Intermittent link-layer errors on management path causing peer keepalive loss",
        "impact": "Failover triggered unnecessarily, redundancy state degraded, brief traffic interruption",
        "subsys": "REDUNDANCY", "daemon": "redundancy_daemon", "debug_file": "heartbeat.c",
        "duration_s": 390, "training_label": "HIGH_HEARTBEAT_FAILURE", "failure_mode": "keepalive_loss",
        "events": [
            ("KEEPALIVE_LOSS_START", "WARN", {"peer": "SECONDARY", "missed": "3"}),
            ("KEEPALIVE_LOSS_SUSTAINED", "HIGH", {"peer": "SECONDARY", "missed": "8"}),
            ("FAILOVER_TRIGGERED", "CRITICAL", {"reason": "keepalive_timeout", "new_active": "LOCAL"}),
            ("FALSE_FAILOVER_DETECTED", "HIGH", {"peer_reachable": "true", "via": "alternate_path"}),
            ("REDUNDANCY_RESTORED", "INFO", {"peer": "SECONDARY", "state": "STANDBY"}),
        ],
        "cascade": [
            ("redundancy_daemon", "Keepalive miss count 3 - peer SECONDARY may be unreachable"),
            ("redundancy_daemon", "HIGH - 8 consecutive keepalive misses from SECONDARY"),
            ("redundancy_daemon", "CRITICAL - Failover initiated: peer declared unreachable"),
            ("redundancy_daemon", "WARNING - False failover detected: peer reachable via alternate path"),
            ("redundancy_daemon", "Redundancy state restored - SECONDARY back to STANDBY"),
        ],
        "metrics": ("Redundancy", ["keepalive_miss_count", "failover_count", "peer_latency_ms"]),
        "signals": ["KEEPALIVE_LOSS", "FAILOVER_TRIGGERED", "FALSE_FAILOVER_DETECTED"],
        "affected": ["redundancy_daemon", "network_monitor", "system_logger"],
    },
    # ── Scenario 17: ACL Policy Corruption ───────────────────────────────────
    {
        "key": "acl_corruption", "title": "ACL Policy Corruption (TCAM Parity Error)",
        "severity": "CRITICAL", "failure_type": "Hardware / TCAM Memory Corruption",
        "root_cause": "TCAM parity error corrupting ACL entries, causing random traffic permit/deny reversal",
        "impact": "Unpredictable traffic forwarding, security policy violation, re-download required",
        "subsys": "ACL", "daemon": "access_control_daemon", "debug_file": "tcam.c",
        "duration_s": 510, "training_label": "CRITICAL_ACL_CORRUPTION", "failure_mode": "tcam_parity_error",
        "events": [
            ("TCAM_PARITY_ERROR", "CRITICAL", {"bank": "0", "address": "0x3F2A", "corrected": "false"}),
            ("ACL_ENTRY_CORRUPTED", "CRITICAL", {"rule_id": "6148", "effect": "permit_deny_inverted"}),
            ("SECURITY_POLICY_VIOLATION", "CRITICAL", {"flow": "blocked_traffic_now_permitted"}),
            ("ACL_DOWNLOAD_INITIATED", "WARN", {"entries_to_reload": "8432"}),
            ("ACL_RESTORE_COMPLETE", "INFO", {"entries_loaded": "8432", "parity_errors": "0"}),
        ],
        "cascade": [
            ("access_control_daemon", "CRITICAL - TCAM parity error detected bank 0 address 0x3F2A"),
            ("access_control_daemon", "CRITICAL - ACL rule 6148 corrupted - security policy violated"),
            ("access_control_daemon", "CRITICAL - Unauthorized traffic being forwarded due to ACL corruption"),
            ("access_control_daemon", "ACL table full re-download initiated: 8432 entries"),
            ("access_control_daemon", "ACL restore complete - all 8432 entries verified clean"),
        ],
        "metrics": ("ACLHealth", ["parity_errors", "corrupted_entries", "policy_violations"]),
        "signals": ["TCAM_PARITY_ERROR", "ACL_ENTRY_CORRUPTED", "SECURITY_POLICY_VIOLATION"],
        "affected": ["access_control_daemon", "network_monitor", "system_logger"],
    },
    # ── Scenario 18: Multicast Flooding ──────────────────────────────────────
    {
        "key": "multicast_flooding", "title": "Multicast Flooding (IGMP Snooping Disabled)",
        "severity": "HIGH", "failure_type": "Multicast / Layer 2 Flooding",
        "root_cause": "IGMP snooping accidentally disabled causing multicast streams to flood all ports",
        "impact": "All ports receiving high-bandwidth multicast, link saturation on non-subscriber VLANs",
        "subsys": "MULTICAST", "daemon": "igmp_daemon", "debug_file": "igmp.c",
        "duration_s": 480, "training_label": "HIGH_MULTICAST_FLOOD", "failure_mode": "multicast_flood",
        "events": [
            ("IGMP_SNOOPING_DISABLED", "WARN", {"vlan": "200", "reason": "config_change"}),
            ("MULTICAST_FLOOD_DETECTED", "HIGH", {"streams": "24", "bandwidth_gbps": "8.4"}),
            ("LINK_SATURATION_WARNING", "HIGH", {"interfaces": "downlink_1,downlink_2", "util_pct": "88"}),
            ("SUBSCRIBER_COMPLAINT_THRESHOLD", "HIGH", {"affected_hosts": "1200"}),
            ("IGMP_SNOOPING_RESTORED", "INFO", {"vlan": "200", "multicast_groups_pruned": "24"}),
        ],
        "cascade": [
            ("igmp_daemon", "IGMP snooping disabled on VLAN 200 - multicast now flooding"),
            ("statistics_collector", "Multicast flood: 24 streams, 8.4 Gbps on all downlinks"),
            ("network_monitor", "downlink_1 and downlink_2 saturation at 88% - multicast flood"),
            ("igmp_daemon", "1200 hosts affected by spurious multicast traffic"),
            ("igmp_daemon", "IGMP snooping re-enabled on VLAN 200 - 24 multicast groups pruned"),
        ],
        "metrics": ("Multicast", ["flood_streams", "multicast_bw_gbps", "affected_hosts"]),
        "signals": ["IGMP_SNOOPING_DISABLED", "MULTICAST_FLOOD_DETECTED", "LINK_SATURATION"],
        "affected": ["igmp_daemon", "statistics_collector", "network_monitor"],
    },
    # ── Scenario 19: Process Watchdog Cascade Failure ─────────────────────────
    {
        "key": "process_cascade_failure", "title": "Process Cascade Failure (Daemon Dependency Chain)",
        "severity": "CRITICAL", "failure_type": "Software / Daemon Dependency Failure",
        "root_cause": "Crash in event_correlator daemon causing dependent daemons to lose event bus and fail",
        "impact": "3 secondary daemons crashed, monitoring blind spot for 7 minutes",
        "subsys": "PROCESS", "daemon": "process_monitor", "debug_file": "event_bus.c",
        "duration_s": 420, "training_label": "CRITICAL_PROCESS_CASCADE", "failure_mode": "daemon_cascade",
        "events": [
            ("EVENT_BUS_FAILURE", "CRITICAL", {"daemon": "event_correlator", "signal": "SIGSEGV"}),
            ("DEPENDENT_DAEMON_TIMEOUT", "CRITICAL", {"daemon": "monitoring_daemon", "reason": "event_bus_lost"}),
            ("SECONDARY_DAEMON_CRASH", "CRITICAL", {"daemon": "statistics_collector", "reason": "ipc_broken"}),
            ("MONITORING_BLIND_SPOT", "CRITICAL", {"duration_s": "420", "daemons_down": "3"}),
            ("DAEMON_RECOVERY_COMPLETE", "INFO", {"restarted": "event_correlator,monitoring_daemon,statistics_collector"}),
        ],
        "cascade": [
            ("event_correlator", "CRITICAL - Segmentation fault - daemon core dumped"),
            ("process_monitor", "CRITICAL - event_correlator terminated: SIGSEGV"),
            ("monitoring_daemon", "CRITICAL - IPC event bus connection lost - entering degraded mode"),
            ("statistics_collector", "CRITICAL - Cannot reach event bus - terminating"),
            ("process_monitor", "All 3 dependent daemons restarted and healthy"),
        ],
        "metrics": ("ProcessHealth", ["daemon_crash_count", "ipc_errors", "monitoring_gap_s"]),
        "signals": ["EVENT_BUS_FAILURE", "DEPENDENT_DAEMON_TIMEOUT", "MONITORING_BLIND_SPOT"],
        "affected": ["event_correlator", "monitoring_daemon", "statistics_collector", "process_monitor"],
    },
    # ── Scenario 20: Clean Baseline (No Anomalies) ────────────────────────────
    {
        "key": "clean_baseline", "title": "Clean Baseline Day (No Anomalies)",
        "severity": "INFO", "failure_type": "Normal Operation",
        "root_cause": "No failure - device operating within all normal parameters all day",
        "impact": "None - zero incidents, zero alerts",
        "subsys": "SYSTEM", "daemon": "monitoring_daemon", "debug_file": "health.c",
        "duration_s": 3600, "training_label": "NORMAL_OPERATION", "failure_mode": "none",
        "events": [
            ("SYSTEM_HEALTH_NORMAL", "INFO", {"cpu_pct": "18", "mem_pct": "45"}),
            ("ALL_INTERFACES_UP", "INFO", {"up_count": "6", "down_count": "0"}),
            ("ROUTING_TABLE_STABLE", "INFO", {"routes": "847", "changes": "0"}),
            ("PEER_SYNC_VERIFIED", "INFO", {"synced_objects": "4782", "sync_state": "SYNCED"}),
            ("DAILY_HEALTH_CHECK_PASSED", "INFO", {"checks_passed": "48", "checks_failed": "0"}),
        ],
        "cascade": [
            ("monitoring_daemon", "All subsystem metrics nominal - no alerts"),
            ("health_check", "System health check passed all subsystems"),
            ("statistics_collector", "Periodic statistics collection complete - all counters normal"),
            ("monitoring_daemon", "Daily health check complete - zero incidents"),
            ("system_logger", "End of day status - no unresolved incidents"),
        ],
        "metrics": ("Baseline", ["cpu_pct", "mem_pct", "link_util_pct"]),
        "signals": ["ALL_CLEAR", "NORMAL_OPERATION"],
        "affected": [],
    },
    # ── Scenario 21: DNS Resolution Failure ──────────────────────────────────
    {
        "key": "dns_resolution_failure", "title": "DNS Resolution Failure (Cache Poisoning + Server Down)",
        "severity": "HIGH", "failure_type": "Name Resolution / DNS Failure",
        "root_cause": "Primary DNS server unreachable and stale cache poisoned causing hostname resolution failures",
        "impact": "Hostname-based ACLs failing, telemetry exports halted, NMS unreachable by name",
        "subsys": "DNS", "daemon": "dns_resolver", "debug_file": "dns.c",
        "duration_s": 720, "training_label": "HIGH_DNS_FAILURE", "failure_mode": "dns_resolution_failure",
        "events": [
            ("DNS_SERVER_UNREACHABLE", "WARN", {"server": "dns.primary", "timeout_ms": "5000"}),
            ("DNS_CACHE_STALE", "HIGH", {"stale_entries": "847", "ttl_expired": "true"}),
            ("HOSTNAME_RESOLUTION_FAILURE", "HIGH", {"host": "nms.internal", "error": "NXDOMAIN"}),
            ("ACL_DNS_LOOKUP_FAILED", "CRITICAL", {"acl_rule": "4821", "host": "nms.internal"}),
            ("DNS_FAILOVER_TO_SECONDARY", "INFO", {"server": "dns.secondary", "latency_ms": "12"}),
        ],
        "cascade": [
            ("dns_resolver", "Primary DNS server dns.primary unreachable - 5000ms timeout"),
            ("dns_resolver", "DNS cache 847 stale entries - TTL expired, cannot refresh"),
            ("dns_resolver", "CRITICAL - Hostname resolution failed for nms.internal: NXDOMAIN"),
            ("access_control_daemon", "CRITICAL - ACL rule 4821 DNS lookup failed - policy enforcement degraded"),
            ("dns_resolver", "Failover to secondary DNS server - resolution restored"),
        ],
        "metrics": ("DNS", ["resolution_latency_ms", "cache_hit_rate", "failure_rate"]),
        "signals": ["DNS_SERVER_UNREACHABLE", "DNS_CACHE_STALE", "HOSTNAME_RESOLUTION_FAILURE"],
        "affected": ["dns_resolver", "access_control_daemon", "telemetry_collector"],
    },
    # ── Scenario 22: ARP Table Exhaustion ────────────────────────────────────
    {
        "key": "arp_table_exhaustion", "title": "ARP Table Exhaustion (Scanning Attack)",
        "severity": "CRITICAL", "failure_type": "Layer 3 / ARP Table Overflow",
        "root_cause": "Rapid ARP scanning attack filling the ARP table causing new hosts to be unreachable",
        "impact": "New hosts cannot be reached, ARP resolution failures for legitimate traffic",
        "subsys": "ARP", "daemon": "arp_daemon", "debug_file": "arp.c",
        "duration_s": 360, "training_label": "CRITICAL_ARP_EXHAUSTION", "failure_mode": "arp_table_full",
        "events": [
            ("ARP_RATE_ANOMALY", "WARN", {"arp_pps": "4800", "normal_pps": "50"}),
            ("ARP_TABLE_HIGH_WATERMARK", "HIGH", {"used": "28000", "capacity": "32768"}),
            ("ARP_TABLE_FULL", "CRITICAL", {"used": "32768", "capacity": "32768", "drops": "true"}),
            ("LEGITIMATE_HOST_UNREACHABLE", "CRITICAL", {"failed_arp_count": "1240"}),
            ("ARP_RATE_LIMIT_APPLIED", "INFO", {"limit_pps": "100", "table_freed": "8200"}),
        ],
        "cascade": [
            ("arp_daemon", "ARP request rate 4800 pps - 96x above normal baseline"),
            ("arp_daemon", "ARP table 85% full - possible scanning attack in progress"),
            ("arp_daemon", "CRITICAL - ARP table FULL 32768/32768 - dropping new requests"),
            ("network_monitor", "CRITICAL - 1240 legitimate hosts unreachable due to ARP exhaustion"),
            ("arp_daemon", "ARP rate limiter applied - table recovery in progress"),
        ],
        "metrics": ("ARP", ["arp_rate_pps", "table_utilization_pct", "drop_count"]),
        "signals": ["ARP_RATE_ANOMALY", "ARP_TABLE_HIGH_WATERMARK", "ARP_TABLE_FULL"],
        "affected": ["arp_daemon", "network_monitor", "forwarding_engine"],
    },
    # ── Scenario 23: Power Supply Unit Failure ────────────────────────────────
    {
        "key": "psu_failure", "title": "Power Supply Failure (PSU Redundancy Degraded)",
        "severity": "CRITICAL", "failure_type": "Hardware / Power Subsystem Failure",
        "root_cause": "PSU-1 failed due to internal over-voltage, device running on single PSU",
        "impact": "Redundant power lost, device at risk if PSU-2 fails, chassis fan speed increased",
        "subsys": "POWER", "daemon": "thermal_monitor", "debug_file": "psu.c",
        "duration_s": 480, "training_label": "CRITICAL_PSU_FAILURE", "failure_mode": "psu_redundancy_lost",
        "events": [
            ("PSU_VOLTAGE_ANOMALY", "WARN", {"psu": "PSU-1", "voltage_v": "11.2", "nominal_v": "12.0"}),
            ("PSU_OVER_VOLTAGE_FAULT", "CRITICAL", {"psu": "PSU-1", "fault_code": "OVP"}),
            ("PSU_FAILURE", "CRITICAL", {"psu": "PSU-1", "state": "FAILED", "output_w": "0"}),
            ("REDUNDANCY_LOST_POWER", "CRITICAL", {"active_psus": "1", "required": "2"}),
            ("PSU_REPLACED_DETECTED", "INFO", {"psu": "PSU-1", "state": "OK", "output_w": "420"}),
        ],
        "cascade": [
            ("thermal_monitor", "PSU-1 output voltage 11.2V below 12.0V nominal - investigating"),
            ("thermal_monitor", "CRITICAL - PSU-1 over-voltage protection fault triggered"),
            ("thermal_monitor", "CRITICAL - PSU-1 FAILED: output 0W - single PSU operation"),
            ("system_logger", "CRITICAL - Power redundancy LOST - chassis operating on single PSU"),
            ("thermal_monitor", "PSU-1 replacement detected - power redundancy restored"),
        ],
        "metrics": ("Power", ["psu1_output_w", "psu2_output_w", "input_voltage_v"]),
        "signals": ["PSU_VOLTAGE_ANOMALY", "PSU_FAILURE", "REDUNDANCY_LOST_POWER"],
        "affected": ["thermal_monitor", "system_logger", "process_monitor"],
    },
    # ── Scenario 24: FIB / Hardware Forwarding Table Exhaustion ───────────────
    {
        "key": "fib_exhaustion", "title": "FIB Table Exhaustion (Route Scale Overflow)",
        "severity": "CRITICAL", "failure_type": "Control Plane / Hardware Table Overflow",
        "root_cause": "Route injection from a misconfigured peer caused FIB table overflow, falling back to software forwarding",
        "impact": "Hardware forwarding disabled, software forwarding fallback causes 10x latency increase",
        "subsys": "FORWARDING", "daemon": "forwarding_engine", "debug_file": "fib.c",
        "duration_s": 540, "training_label": "CRITICAL_FIB_EXHAUSTION", "failure_mode": "fib_overflow",
        "events": [
            ("ROUTE_COUNT_WARNING", "WARN", {"fib_entries": "800000", "capacity": "1000000"}),
            ("ROUTE_INJECT_SPIKE", "HIGH", {"new_routes_per_s": "12000", "peer": "PEER_B"}),
            ("FIB_TABLE_FULL", "CRITICAL", {"fib_entries": "1000000", "capacity": "1000000"}),
            ("SW_FORWARDING_FALLBACK", "CRITICAL", {"latency_increase_x": "10", "pps_drop_pct": "60"}),
            ("PEER_ROUTE_FILTERED", "INFO", {"peer": "PEER_B", "routes_withdrawn": "210000"}),
        ],
        "cascade": [
            ("forwarding_engine", "FIB utilization 80% - route scale approaching hardware limit"),
            ("routing_daemon", "Route injection rate 12000/s from PEER_B - abnormal"),
            ("forwarding_engine", "CRITICAL - FIB table FULL 1M/1M entries - overflow condition"),
            ("forwarding_engine", "CRITICAL - Falling back to software forwarding - throughput degraded"),
            ("routing_daemon", "Route filter applied to PEER_B - 210K routes withdrawn"),
        ],
        "metrics": ("FIB", ["fib_entries", "hw_forwarding_pps", "sw_forwarding_pps"]),
        "signals": ["ROUTE_COUNT_WARNING", "FIB_TABLE_FULL", "SW_FORWARDING_FALLBACK"],
        "affected": ["forwarding_engine", "routing_daemon", "statistics_collector"],
    },
    # ── Scenario 25: LACP Bond Degradation ───────────────────────────────────
    {
        "key": "lacp_bond_degradation", "title": "LACP Bond Degradation (Member Link Failures)",
        "severity": "HIGH", "failure_type": "Link Aggregation / LACP Bond Failure",
        "root_cause": "Two of four LACP bond member links failed, reducing aggregate bandwidth by 50%",
        "impact": "50% bandwidth reduction on uplink bond, traffic overload on surviving members",
        "subsys": "LAG", "daemon": "lacp_daemon", "debug_file": "lacp.c",
        "duration_s": 600, "training_label": "HIGH_LACP_DEGRADATION", "failure_mode": "bond_member_failure",
        "events": [
            ("LACP_MEMBER_DOWN", "HIGH", {"interface": "uplink_3", "bond": "bond0", "members_remaining": "3"}),
            ("LACP_MEMBER_DOWN_2", "HIGH", {"interface": "uplink_4", "bond": "bond0", "members_remaining": "2"}),
            ("BOND_BANDWIDTH_REDUCED", "HIGH", {"bond": "bond0", "bw_gbps": "20", "original_gbps": "40"}),
            ("TRAFFIC_OVERLOAD_SURVIVING", "CRITICAL", {"interface": "uplink_1", "util_pct": "94"}),
            ("LACP_MEMBERS_RESTORED", "INFO", {"bond": "bond0", "members_active": "4"}),
        ],
        "cascade": [
            ("lacp_daemon", "LACP member uplink_3 down - bond0 degraded to 3 members"),
            ("lacp_daemon", "LACP member uplink_4 down - bond0 degraded to 2 members"),
            ("lacp_daemon", "HIGH - Bond bond0 bandwidth reduced to 20Gbps (was 40Gbps)"),
            ("statistics_collector", "CRITICAL - uplink_1 utilization 94% due to LACP bond degradation"),
            ("lacp_daemon", "All 4 LACP members restored - bond0 fully operational"),
        ],
        "metrics": ("LAG", ["bond_member_count", "bond_bw_gbps", "member_util_pct"]),
        "signals": ["LACP_MEMBER_DOWN", "BOND_BANDWIDTH_REDUCED", "TRAFFIC_OVERLOAD_SURVIVING"],
        "affected": ["lacp_daemon", "statistics_collector", "network_monitor"],
    },
    # ── Scenario 26: SSL/TLS Certificate Expiry ───────────────────────────────
    {
        "key": "cert_expiry", "title": "SSL/TLS Certificate Expiry (Management Auth Failure)",
        "severity": "HIGH", "failure_type": "Security / Certificate Management Failure",
        "root_cause": "Management plane TLS certificate expired causing HTTPS and NETCONF sessions to reject",
        "impact": "All HTTPS/NETCONF management sessions rejected, NMS cannot connect",
        "subsys": "SECURITY", "daemon": "config_daemon", "debug_file": "tls.c",
        "duration_s": 3600, "training_label": "HIGH_CERT_EXPIRY", "failure_mode": "cert_expired",
        "events": [
            ("CERT_EXPIRY_WARNING", "WARN", {"cert": "mgmt_tls", "days_remaining": "7"}),
            ("CERT_EXPIRY_IMMINENT", "HIGH", {"cert": "mgmt_tls", "days_remaining": "1"}),
            ("CERT_EXPIRED", "CRITICAL", {"cert": "mgmt_tls", "expired_at": "00:00:00"}),
            ("MGMT_SESSION_REJECTED", "CRITICAL", {"sessions_rejected": "42", "reason": "cert_expired"}),
            ("CERT_RENEWED", "INFO", {"cert": "mgmt_tls", "valid_days": "365"}),
        ],
        "cascade": [
            ("config_daemon", "TLS certificate mgmt_tls expires in 7 days - renewal required"),
            ("config_daemon", "WARNING - TLS certificate mgmt_tls expires in 24 hours"),
            ("config_daemon", "CRITICAL - TLS certificate mgmt_tls EXPIRED - rejecting all TLS connections"),
            ("monitoring_daemon", "CRITICAL - 42 management sessions rejected due to expired certificate"),
            ("config_daemon", "New TLS certificate installed - management sessions restored"),
        ],
        "metrics": ("TLSCert", ["cert_days_remaining", "rejected_sessions", "tls_handshake_failures"]),
        "signals": ["CERT_EXPIRY_WARNING", "CERT_EXPIRED", "MGMT_SESSION_REJECTED"],
        "affected": ["config_daemon", "monitoring_daemon", "access_control_daemon"],
    },
    # ── Scenario 27: OSPF Neighbor Loss / Re-convergence ─────────────────────
    {
        "key": "ospf_neighbor_loss", "title": "OSPF Neighbor Loss (Dead Timer Expiry)",
        "severity": "HIGH", "failure_type": "Routing Protocol / OSPF Convergence Failure",
        "root_cause": "BFD session loss caused by CPU spike triggered OSPF neighbor to expire dead timer",
        "impact": "OSPF re-convergence, route flap, 12s traffic disruption on affected subnets",
        "subsys": "ROUTING", "daemon": "routing_daemon", "debug_file": "ospf.c",
        "duration_s": 480, "training_label": "HIGH_OSPF_CONVERGENCE_FAILURE", "failure_mode": "ospf_neighbor_loss",
        "events": [
            ("BFD_SESSION_DOWN", "HIGH", {"peer": "10.0.1.2", "reason": "control_detection_expired"}),
            ("OSPF_DEAD_TIMER_EXPIRED", "CRITICAL", {"neighbor": "10.0.1.2", "area": "0.0.0.0"}),
            ("OSPF_NEIGHBOR_DOWN", "CRITICAL", {"neighbor": "10.0.1.2", "prev_state": "FULL"}),
            ("ROUTE_WITHDRAWAL", "HIGH", {"withdrawn_routes": "384", "area": "0.0.0.0"}),
            ("OSPF_NEIGHBOR_RESTORED", "INFO", {"neighbor": "10.0.1.2", "state": "FULL", "convergence_s": "12"}),
        ],
        "cascade": [
            ("routing_daemon", "BFD session to 10.0.1.2 DOWN - control detection time expired"),
            ("routing_daemon", "CRITICAL - OSPF dead timer expired for neighbor 10.0.1.2 area 0.0.0.0"),
            ("routing_daemon", "CRITICAL - OSPF neighbor 10.0.1.2 state change FULL -> DOWN"),
            ("routing_daemon", "384 routes withdrawn from RIB - reconvergence in progress"),
            ("routing_daemon", "OSPF neighbor 10.0.1.2 restored - FULL state after 12s convergence"),
        ],
        "metrics": ("OSPF", ["neighbor_count", "routes_in_rib", "convergence_ms"]),
        "signals": ["BFD_SESSION_DOWN", "OSPF_DEAD_TIMER_EXPIRED", "ROUTE_WITHDRAWAL"],
        "affected": ["routing_daemon", "forwarding_engine", "statistics_collector"],
    },
    # ── Scenario 28: Slow Memory Leak (Gradual Drift) ─────────────────────────
    {
        "key": "memory_leak_gradual", "title": "Slow Memory Leak (Gradual Heap Growth)",
        "severity": "HIGH", "failure_type": "Resource Exhaustion / Gradual Memory Leak",
        "root_cause": "Routing daemon slow memory leak accumulating over 8 hours causing OOM risk",
        "impact": "Available memory declining 0.5% per hour, swap pressure starting, risk of OOM",
        "subsys": "MEMORY", "daemon": "memory_manager", "debug_file": "memory.c",
        "duration_s": 28800, "training_label": "HIGH_GRADUAL_MEMORY_LEAK", "failure_mode": "memory_leak_gradual",
        "events": [
            ("HEAP_GROWTH_DETECTED", "WARN", {"daemon": "routing_daemon", "growth_mb_per_h": "42"}),
            ("MEMORY_UTIL_ELEVATED", "WARN", {"mem_pct": "68", "normal_pct": "45"}),
            ("SWAP_PRESSURE_START", "HIGH", {"swap_used_mb": "512", "swap_total_mb": "4096"}),
            ("MEMORY_UTIL_CRITICAL", "CRITICAL", {"mem_pct": "89", "free_mb": "900"}),
            ("DAEMON_RESTART_LEAK_FIX", "INFO", {"daemon": "routing_daemon", "mem_freed_mb": "3400"}),
        ],
        "cascade": [
            ("memory_manager", "routing_daemon heap growing at 42 MB/hour - possible leak"),
            ("memory_manager", "System memory utilization 68% - above normal 45% baseline"),
            ("memory_manager", "HIGH - Swap usage started: 512MB used - memory pressure increasing"),
            ("memory_manager", "CRITICAL - System memory 89% utilized - OOM risk elevated"),
            ("process_monitor", "routing_daemon restarted to clear leak - 3400 MB freed"),
        ],
        "metrics": ("MemoryLeak", ["mem_util_pct", "heap_growth_mb", "swap_used_mb"]),
        "signals": ["HEAP_GROWTH_DETECTED", "SWAP_PRESSURE_START", "MEMORY_UTIL_CRITICAL"],
        "affected": ["memory_manager", "routing_daemon", "process_monitor"],
    },
    # ── Scenario 29: MTU Mismatch (Jumbo Frame Black Hole) ────────────────────
    {
        "key": "mtu_mismatch", "title": "MTU Mismatch (Jumbo Frame Black Hole)",
        "severity": "HIGH", "failure_type": "Network Configuration / MTU Mismatch",
        "root_cause": "MTU changed to 9000 on local device but not on peer, causing silent drop of large frames",
        "impact": "Large packets silently dropped, TCP throughput collapsed, applications timing out",
        "subsys": "INTERFACE", "daemon": "interface_manager", "debug_file": "mtu.c",
        "duration_s": 900, "training_label": "HIGH_MTU_MISMATCH", "failure_mode": "mtu_black_hole",
        "events": [
            ("MTU_CONFIG_CHANGE", "INFO", {"interface": "uplink_1", "new_mtu": "9000", "old_mtu": "1500"}),
            ("LARGE_FRAME_DROP_DETECTED", "HIGH", {"interface": "uplink_1", "drop_pct": "34"}),
            ("TCP_THROUGHPUT_COLLAPSE", "HIGH", {"throughput_mbps": "12", "expected_mbps": "800"}),
            ("ICMP_FRAG_NEEDED", "HIGH", {"count": "48000", "interval_s": "60"}),
            ("MTU_ROLLBACK", "INFO", {"interface": "uplink_1", "mtu": "1500", "traffic_restored": "true"}),
        ],
        "cascade": [
            ("interface_manager", "MTU changed to 9000 on uplink_1 - jumbo frames enabled"),
            ("statistics_collector", "Large frame drop rate 34% on uplink_1 - MTU mismatch suspected"),
            ("network_monitor", "TCP throughput collapsed to 12 Mbps on uplink_1 - investigating"),
            ("interface_manager", "HIGH - 48000 ICMP Frag-Needed messages in 60s - path MTU issue"),
            ("interface_manager", "MTU rolled back to 1500 on uplink_1 - traffic restored"),
        ],
        "metrics": ("MTU", ["large_frame_drops", "tcp_throughput_mbps", "frag_needed_count"]),
        "signals": ["LARGE_FRAME_DROP_DETECTED", "TCP_THROUGHPUT_COLLAPSE", "ICMP_FRAG_NEEDED"],
        "affected": ["interface_manager", "statistics_collector", "network_monitor"],
    },
    # ── Scenario 30: SSH Brute Force Attack ───────────────────────────────────
    {
        "key": "ssh_brute_force", "title": "SSH Brute Force Attack (Login Flood)",
        "severity": "HIGH", "failure_type": "Security / Authentication Attack",
        "root_cause": "SSH login flood from external IP exhausting management CPU and authentication daemon",
        "impact": "Auth daemon backlogged, legitimate admin access delayed, auth log volume explosion",
        "subsys": "SECURITY", "daemon": "access_control_daemon", "debug_file": "auth.c",
        "duration_s": 600, "training_label": "HIGH_SSH_BRUTE_FORCE", "failure_mode": "auth_flood",
        "events": [
            ("SSH_LOGIN_RATE_SPIKE", "HIGH", {"attempts_per_min": "3600", "source_ip": "192.0.2.47"}),
            ("AUTH_DAEMON_BACKLOG", "HIGH", {"queue_depth": "480", "max_queue": "512"}),
            ("ADMIN_ACCESS_DELAYED", "HIGH", {"delay_s": "28", "reason": "auth_queue_full"}),
            ("ACCOUNT_LOCKOUT_TRIGGERED", "CRITICAL", {"accounts_locked": "3", "source_ip": "192.0.2.47"}),
            ("SSH_RATE_LIMIT_APPLIED", "INFO", {"source_ip": "192.0.2.47", "action": "blocked_for_300s"}),
        ],
        "cascade": [
            ("access_control_daemon", "SSH login attempt rate 3600/min from 192.0.2.47 - brute force suspected"),
            ("access_control_daemon", "HIGH - Authentication daemon queue 480/512 - near capacity"),
            ("access_control_daemon", "HIGH - Legitimate admin login delayed 28s due to auth backlog"),
            ("access_control_daemon", "CRITICAL - 3 accounts locked out due to failed login threshold"),
            ("access_control_daemon", "Rate limit applied: 192.0.2.47 blocked for 300 seconds"),
        ],
        "metrics": ("AuthSecurity", ["ssh_attempt_rate", "auth_queue_depth", "lockout_count"]),
        "signals": ["SSH_LOGIN_RATE_SPIKE", "AUTH_DAEMON_BACKLOG", "ACCOUNT_LOCKOUT_TRIGGERED"],
        "affected": ["access_control_daemon", "monitoring_daemon", "system_logger"],
    },
    # ── Scenario 31: QoS Misconfiguration (Voice Traffic Drops) ──────────────
    {
        "key": "qos_misconfiguration", "title": "QoS Misconfiguration (Voice Traffic Drops)",
        "severity": "HIGH", "failure_type": "QoS / Traffic Scheduling Failure",
        "root_cause": "QoS policy update incorrectly classified voice (DSCP EF) into best-effort queue",
        "impact": "Voice calls dropping packets at 8%, real-time traffic latency >150ms, SLA breached",
        "subsys": "QOS", "daemon": "qos_scheduler_daemon", "debug_file": "qos.c",
        "duration_s": 780, "training_label": "HIGH_QOS_MISCONFIG", "failure_mode": "qos_misclassification",
        "events": [
            ("QOS_POLICY_APPLIED", "INFO", {"policy": "v2.4.1", "change_id": "CHG-9012"}),
            ("VOICE_QUEUE_DROPS", "HIGH", {"queue": "be_queue", "drop_pct": "8.2", "dscp": "EF"}),
            ("VOICE_LATENCY_BREACH", "HIGH", {"latency_ms": "162", "sla_ms": "150"}),
            ("DSCP_MISCLASSIFICATION", "CRITICAL", {"dscp": "EF", "mapped_queue": "be_queue", "expected_queue": "ef_queue"}),
            ("QOS_POLICY_REVERTED", "INFO", {"policy": "v2.4.0", "voice_drops": "0"}),
        ],
        "cascade": [
            ("qos_scheduler_daemon", "QoS policy v2.4.1 applied - CHG-9012"),
            ("qos_scheduler_daemon", "HIGH - Voice queue drop rate 8.2% on BE queue - DSCP EF misclassified"),
            ("statistics_collector", "HIGH - Real-time traffic latency 162ms exceeds 150ms SLA"),
            ("qos_scheduler_daemon", "CRITICAL - DSCP EF traffic mapped to best-effort - policy error"),
            ("qos_scheduler_daemon", "QoS policy reverted to v2.4.0 - voice traffic restored"),
        ],
        "metrics": ("QoS", ["voice_drop_pct", "voice_latency_ms", "queue_utilization_pct"]),
        "signals": ["VOICE_QUEUE_DROPS", "VOICE_LATENCY_BREACH", "DSCP_MISCLASSIFICATION"],
        "affected": ["qos_scheduler_daemon", "statistics_collector", "network_monitor"],
    },
    # ── Scenario 32: Storm Control Triggered (Broadcast Flood) ────────────────
    {
        "key": "storm_control_triggered", "title": "Storm Control Triggered (Broadcast Flood)",
        "severity": "HIGH", "failure_type": "Layer 2 / Storm Control Activation",
        "root_cause": "Malfunctioning end device sending continuous broadcasts triggering storm control suppression",
        "impact": "Storm control suppressing 70% of traffic on downlink_2, affected hosts unreachable",
        "subsys": "STP", "daemon": "qos_scheduler_daemon", "debug_file": "storm.c",
        "duration_s": 420, "training_label": "HIGH_STORM_CONTROL", "failure_mode": "broadcast_storm_suppressed",
        "events": [
            ("BROADCAST_RATE_HIGH", "WARN", {"interface": "downlink_2", "bcast_pps": "18000", "threshold_pps": "5000"}),
            ("STORM_CONTROL_ACTIVATED", "HIGH", {"interface": "downlink_2", "suppression_pct": "70"}),
            ("HOSTS_UNREACHABLE", "HIGH", {"affected_hosts": "420", "interface": "downlink_2"}),
            ("STORM_SUSTAINED", "CRITICAL", {"interface": "downlink_2", "duration_s": "240"}),
            ("FAULTY_DEVICE_ISOLATED", "INFO", {"port": "downlink_2", "action": "err_disabled"}),
        ],
        "cascade": [
            ("qos_scheduler_daemon", "Broadcast rate 18000 pps on downlink_2 exceeds 5000 pps threshold"),
            ("qos_scheduler_daemon", "HIGH - Storm control activated on downlink_2 - suppressing 70% traffic"),
            ("network_monitor", "HIGH - 420 hosts unreachable on segment behind downlink_2"),
            ("qos_scheduler_daemon", "CRITICAL - Storm sustained 240s - port isolation warranted"),
            ("interface_manager", "Port downlink_2 err-disabled - faulty device isolated"),
        ],
        "metrics": ("StormCtrl", ["bcast_pps", "suppression_pct", "affected_hosts"]),
        "signals": ["BROADCAST_RATE_HIGH", "STORM_CONTROL_ACTIVATED", "STORM_SUSTAINED"],
        "affected": ["qos_scheduler_daemon", "interface_manager", "network_monitor"],
    },
    # ── Scenario 33: Syslog Server Unreachable ────────────────────────────────
    {
        "key": "syslog_server_unreachable", "title": "Remote Syslog Server Unreachable (Log Blackout)",
        "severity": "HIGH", "failure_type": "Observability / Logging Failure",
        "root_cause": "Remote syslog server network path failed causing on-device log buffer overflow",
        "impact": "Remote logging halted, on-device buffer overflow with oldest logs lost, audit gap created",
        "subsys": "LOGGING", "daemon": "system_logger", "debug_file": "syslog.c",
        "duration_s": 1800, "training_label": "HIGH_SYSLOG_FAILURE", "failure_mode": "syslog_blackout",
        "events": [
            ("SYSLOG_SERVER_UNREACHABLE", "WARN", {"server": "syslog.corp", "port": "514", "retries": "3"}),
            ("LOCAL_BUFFER_FILLING", "HIGH", {"buffer_pct": "72", "rate_per_min": "840"}),
            ("LOCAL_BUFFER_OVERFLOW", "CRITICAL", {"lost_logs": "12400", "oldest_lost": "30min_ago"}),
            ("AUDIT_GAP_CREATED", "CRITICAL", {"gap_minutes": "30", "compliance_impact": "true"}),
            ("SYSLOG_SERVER_RESTORED", "INFO", {"server": "syslog.corp", "buffered_logs_flushed": "true"}),
        ],
        "cascade": [
            ("system_logger", "Remote syslog server syslog.corp:514 unreachable - 3 retries failed"),
            ("system_logger", "LOCAL BUFFER 72% full - logging to remote server failing"),
            ("system_logger", "CRITICAL - Local log buffer overflow - 12400 log entries lost"),
            ("system_logger", "CRITICAL - 30 minute audit gap created - compliance risk"),
            ("system_logger", "Remote syslog restored - buffered logs flushed to server"),
        ],
        "metrics": ("Syslog", ["remote_log_failures", "local_buffer_pct", "lost_log_count"]),
        "signals": ["SYSLOG_SERVER_UNREACHABLE", "LOCAL_BUFFER_OVERFLOW", "AUDIT_GAP_CREATED"],
        "affected": ["system_logger", "monitoring_daemon", "telemetry_collector"],
    },
    # ── Scenario 34: Control Plane Policing Overrun ───────────────────────────
    {
        "key": "copp_overrun", "title": "Control Plane Policing Overrun (Routing Protocol Starvation)",
        "severity": "CRITICAL", "failure_type": "Control Plane / CoPP Policy Overrun",
        "root_cause": "Misconfigured CoPP policy let data-plane traffic leak into control plane queue, starving routing protocols",
        "impact": "Routing protocol hellos dropped, 3 BGP sessions reset, partial route loss",
        "subsys": "ACL", "daemon": "access_control_daemon", "debug_file": "copp.c",
        "duration_s": 330, "training_label": "CRITICAL_COPP_OVERRUN", "failure_mode": "control_plane_starvation",
        "events": [
            ("COPP_QUEUE_OVERRUN", "CRITICAL", {"queue": "routing_protocol", "drop_pct": "62"}),
            ("BGP_HELLO_DROPS", "CRITICAL", {"dropped_hellos": "840", "interval_s": "30"}),
            ("BGP_SESSION_RESET", "CRITICAL", {"sessions_reset": "3", "reason": "hold_timer_expired"}),
            ("ROUTE_TABLE_INSTABILITY", "CRITICAL", {"withdrawn_routes": "12000"}),
            ("COPP_POLICY_CORRECTED", "INFO", {"queue": "routing_protocol", "policy": "v3.1"}),
        ],
        "cascade": [
            ("access_control_daemon", "CRITICAL - CoPP routing_protocol queue 62% drops - starvation"),
            ("routing_daemon", "CRITICAL - BGP hello drops detected: 840 in 30s due to CoPP"),
            ("routing_daemon", "CRITICAL - 3 BGP sessions reset: hold timer expired"),
            ("routing_daemon", "CRITICAL - 12000 routes withdrawn due to BGP session resets"),
            ("access_control_daemon", "CoPP policy updated to v3.1 - routing protocol queue protected"),
        ],
        "metrics": ("CoPP", ["copp_drop_pct", "bgp_hello_drops", "sessions_reset"]),
        "signals": ["COPP_QUEUE_OVERRUN", "BGP_HELLO_DROPS", "BGP_SESSION_RESET"],
        "affected": ["access_control_daemon", "routing_daemon", "forwarding_engine"],
    },
    # ── Scenario 35: SNMP Trap Storm ──────────────────────────────────────────
    {
        "key": "snmp_trap_storm", "title": "SNMP Trap Storm (NMS Flooding)",
        "severity": "HIGH", "failure_type": "Management Plane / SNMP Trap Flooding",
        "root_cause": "Misconfigured trap rate limit caused device to flood NMS with repeated traps, saturating management channel",
        "impact": "NMS trap queue full, real traps delayed or dropped, management visibility compromised",
        "subsys": "TELEMETRY", "daemon": "telemetry_collector", "debug_file": "snmp.c",
        "duration_s": 540, "training_label": "HIGH_SNMP_TRAP_STORM", "failure_mode": "snmp_flood",
        "events": [
            ("SNMP_TRAP_RATE_HIGH", "WARN", {"traps_per_s": "4800", "normal_per_s": "10"}),
            ("NMS_QUEUE_FILLING", "HIGH", {"nms_queue_pct": "88"}),
            ("REAL_TRAP_DELAY", "HIGH", {"delay_s": "180", "reason": "nms_queue_full"}),
            ("MGMT_CHANNEL_SATURATED", "CRITICAL", {"bandwidth_util_pct": "96", "channel": "mgmt_0"}),
            ("SNMP_RATE_LIMIT_APPLIED", "INFO", {"limit_traps_per_s": "50", "trap_storm_cleared": "true"}),
        ],
        "cascade": [
            ("telemetry_collector", "SNMP trap rate 4800/s - 480x above normal 10/s baseline"),
            ("telemetry_collector", "HIGH - NMS trap processing queue 88% full"),
            ("telemetry_collector", "HIGH - Real operational traps delayed 180s due to storm"),
            ("telemetry_collector", "CRITICAL - Management channel mgmt_0 at 96% - SNMP flood"),
            ("telemetry_collector", "SNMP trap rate limit applied: 50/s - storm cleared"),
        ],
        "metrics": ("SNMP", ["trap_rate_per_s", "nms_queue_pct", "real_trap_delay_s"]),
        "signals": ["SNMP_TRAP_RATE_HIGH", "NMS_QUEUE_FILLING", "MGMT_CHANNEL_SATURATED"],
        "affected": ["telemetry_collector", "monitoring_daemon", "statistics_collector"],
    },
    # ── Scenario 36: Graceful Restart Failure ─────────────────────────────────
    {
        "key": "graceful_restart_failure", "title": "Graceful Restart Failure (Helper Mode Timeout)",
        "severity": "HIGH", "failure_type": "Routing Protocol / Graceful Restart Failure",
        "root_cause": "Routing daemon graceful restart exceeded helper timeout causing full adjacency reset",
        "impact": "Full BGP convergence instead of graceful, traffic black-hole during re-convergence",
        "subsys": "ROUTING", "daemon": "routing_daemon", "debug_file": "gr.c",
        "duration_s": 420, "training_label": "HIGH_GRACEFUL_RESTART_FAILURE", "failure_mode": "gr_timeout",
        "events": [
            ("GRACEFUL_RESTART_INITIATED", "INFO", {"peer": "PEER_C", "restart_time_s": "120"}),
            ("GR_HELPER_TIMER_RUNNING", "WARN", {"elapsed_s": "90", "timeout_s": "120"}),
            ("GR_HELPER_TIMEOUT", "CRITICAL", {"peer": "PEER_C", "elapsed_s": "121"}),
            ("FULL_SESSION_RESET", "CRITICAL", {"peer": "PEER_C", "routes_withdrawn": "8400"}),
            ("CONVERGENCE_COMPLETE", "INFO", {"peer": "PEER_C", "routes_restored": "8400", "duration_s": "28"}),
        ],
        "cascade": [
            ("routing_daemon", "Graceful restart initiated for peer PEER_C - helper mode active"),
            ("routing_daemon", "GR helper timer: 90s elapsed of 120s timeout - restart slow"),
            ("routing_daemon", "CRITICAL - GR helper timeout for PEER_C at 121s - ending helper mode"),
            ("routing_daemon", "CRITICAL - Full session reset: 8400 routes withdrawn from PEER_C"),
            ("routing_daemon", "Re-convergence complete - 8400 routes restored in 28 seconds"),
        ],
        "metrics": ("GracefulRestart", ["gr_elapsed_s", "routes_withdrawn", "convergence_s"]),
        "signals": ["GR_HELPER_TIMEOUT", "FULL_SESSION_RESET"],
        "affected": ["routing_daemon", "forwarding_engine", "network_monitor"],
    },
    # ── Scenario 37: IPv6 RA Flooding (Rogue RA) ──────────────────────────────
    {
        "key": "ipv6_ra_flooding", "title": "IPv6 Router Advertisement Flooding (Rogue RA)",
        "severity": "HIGH", "failure_type": "IPv6 / Rogue Router Advertisement Attack",
        "root_cause": "Misconfigured host sending unauthorized IPv6 RAs disrupting address assignment",
        "impact": "Hosts receiving rogue default gateway, IPv6 traffic blackholed on affected VLAN",
        "subsys": "ROUTING", "daemon": "routing_daemon", "debug_file": "ipv6.c",
        "duration_s": 660, "training_label": "HIGH_ROGUE_RA", "failure_mode": "ipv6_ra_flood",
        "events": [
            ("RA_RATE_ANOMALY", "WARN", {"ra_per_min": "1800", "normal_per_min": "2", "vlan": "300"}),
            ("ROGUE_RA_DETECTED", "HIGH", {"source_mac": "aa:bb:cc:dd:ee:ff", "vlan": "300"}),
            ("HOST_DEFAULT_GW_POISONED", "CRITICAL", {"affected_hosts": "680", "vlan": "300"}),
            ("IPV6_TRAFFIC_BLACKHOLE", "CRITICAL", {"vlan": "300", "drop_pct": "100"}),
            ("RA_GUARD_APPLIED", "INFO", {"vlan": "300", "rogue_port": "downlink_6", "action": "blocked"}),
        ],
        "cascade": [
            ("routing_daemon", "IPv6 RA rate 1800/min on VLAN 300 - 900x above normal"),
            ("routing_daemon", "HIGH - Rogue RA source aa:bb:cc:dd:ee:ff detected on VLAN 300"),
            ("routing_daemon", "CRITICAL - 680 hosts have rogue default gateway on VLAN 300"),
            ("network_monitor", "CRITICAL - IPv6 traffic BLACKHOLED on VLAN 300 via rogue gateway"),
            ("routing_daemon", "RA Guard applied on downlink_6 - rogue RA blocked"),
        ],
        "metrics": ("IPv6RA", ["ra_rate_per_min", "affected_hosts", "ipv6_drop_pct"]),
        "signals": ["RA_RATE_ANOMALY", "ROGUE_RA_DETECTED", "IPV6_TRAFFIC_BLACKHOLE"],
        "affected": ["routing_daemon", "network_monitor", "interface_manager"],
    },
    # ── Scenario 38: Hardware ASIC Error (Parity / ECC) ──────────────────────
    {
        "key": "asic_ecc_error", "title": "Hardware ASIC ECC Memory Error",
        "severity": "CRITICAL", "failure_type": "Hardware / ECC Memory Failure",
        "root_cause": "Uncorrectable ECC error in packet buffer ASIC causing packet corruption and partial forwarding outage",
        "impact": "Random packet corruption, 5% of forwarded packets corrupted, silent data corruption",
        "subsys": "PHYSICAL", "daemon": "physical_monitor", "debug_file": "asic_ecc.c",
        "duration_s": 600, "training_label": "CRITICAL_ASIC_ECC_ERROR", "failure_mode": "ecc_uncorrectable",
        "events": [
            ("ECC_CORRECTABLE_ERROR", "WARN", {"asic": "pkt_buf_0", "corrected": "true", "count": "12"}),
            ("ECC_ERROR_RATE_RISING", "HIGH", {"asic": "pkt_buf_0", "errors_per_min": "480"}),
            ("ECC_UNCORRECTABLE_ERROR", "CRITICAL", {"asic": "pkt_buf_0", "corrected": "false"}),
            ("PACKET_CORRUPTION_DETECTED", "CRITICAL", {"corrupt_pct": "4.8", "crc_errors": "9200"}),
            ("ASIC_FAILOVER_INITIATED", "INFO", {"asic": "pkt_buf_0", "standby": "pkt_buf_1"}),
        ],
        "cascade": [
            ("physical_monitor", "ECC correctable error in pkt_buf_0 - 12 errors corrected"),
            ("physical_monitor", "HIGH - ECC error rate rising: 480 errors/min in pkt_buf_0"),
            ("physical_monitor", "CRITICAL - Uncorrectable ECC error in pkt_buf_0 - data integrity risk"),
            ("statistics_collector", "CRITICAL - Packet corruption: 4.8% CRC errors on forwarded traffic"),
            ("physical_monitor", "ASIC failover: pkt_buf_0 -> pkt_buf_1 - packet integrity restored"),
        ],
        "metrics": ("ASICHealth", ["ecc_error_rate", "corrupt_pkt_pct", "crc_error_count"]),
        "signals": ["ECC_ERROR_RATE_RISING", "ECC_UNCORRECTABLE_ERROR", "PACKET_CORRUPTION_DETECTED"],
        "affected": ["physical_monitor", "forwarding_engine", "statistics_collector"],
    },
    # ── Scenario 39: Config Sync Failure (Active-Standby Mismatch) ───────────
    {
        "key": "config_sync_failure", "title": "Config Sync Failure (Active-Standby Divergence)",
        "severity": "HIGH", "failure_type": "High Availability / Configuration Synchronization Failure",
        "root_cause": "Config sync channel failure caused running config to diverge between active and standby",
        "impact": "Standby has 47 stale config lines, failover would result in configuration divergence",
        "subsys": "REDUNDANCY", "daemon": "redundancy_daemon", "debug_file": "config_sync.c",
        "duration_s": 720, "training_label": "HIGH_CONFIG_SYNC_FAILURE", "failure_mode": "config_divergence",
        "events": [
            ("CONFIG_SYNC_CHANNEL_LOST", "WARN", {"channel": "cfg_sync_link", "reason": "link_error"}),
            ("CONFIG_SYNC_RETRIES_EXHAUSTED", "HIGH", {"attempts": "10", "interval_s": "30"}),
            ("CONFIG_DIVERGENCE_DETECTED", "CRITICAL", {"diverged_lines": "47", "standby": "SECONDARY"}),
            ("FAILOVER_RISK_HIGH", "CRITICAL", {"reason": "standby_config_stale", "risk_level": "HIGH"}),
            ("CONFIG_SYNC_RESTORED", "INFO", {"channel": "cfg_sync_link", "lines_synced": "47"}),
        ],
        "cascade": [
            ("redundancy_daemon", "Config sync channel cfg_sync_link lost - link error"),
            ("redundancy_daemon", "HIGH - Config sync failed after 10 retries - standby diverging"),
            ("redundancy_daemon", "CRITICAL - Config divergence: standby SECONDARY has 47 stale lines"),
            ("redundancy_daemon", "CRITICAL - Failover risk HIGH: standby config diverged from active"),
            ("redundancy_daemon", "Config sync restored - 47 diverged lines re-synced to standby"),
        ],
        "metrics": ("ConfigSync", ["sync_failures", "diverged_lines", "sync_lag_s"]),
        "signals": ["CONFIG_SYNC_CHANNEL_LOST", "CONFIG_DIVERGENCE_DETECTED", "FAILOVER_RISK_HIGH"],
        "affected": ["redundancy_daemon", "config_daemon", "system_logger"],
    },
    # ── Scenario 40: Second Clean Baseline (Weekend Low Traffic) ─────────────
    {
        "key": "clean_baseline_weekend", "title": "Clean Baseline (Weekend Low Traffic)",
        "severity": "INFO", "failure_type": "Normal Operation",
        "root_cause": "No failure - weekend operation with reduced traffic, all subsystems healthy",
        "impact": "None - zero incidents, lower utilization than weekday baseline",
        "subsys": "SYSTEM", "daemon": "monitoring_daemon", "debug_file": "health.c",
        "duration_s": 3600, "training_label": "NORMAL_OPERATION_LOW_TRAFFIC", "failure_mode": "none",
        "events": [
            ("WEEKEND_LOW_TRAFFIC_MODE", "INFO", {"traffic_reduction_pct": "60"}),
            ("ALL_INTERFACES_UP", "INFO", {"up_count": "6", "down_count": "0"}),
            ("CPU_IDLE_WEEKEND", "INFO", {"cpu_pct": "8", "load_1m": "0.12"}),
            ("MEMORY_NORMAL", "INFO", {"mem_pct": "42", "free_mb": "4800"}),
            ("WEEKEND_HEALTH_CHECK_PASSED", "INFO", {"checks_passed": "48", "checks_failed": "0"}),
        ],
        "cascade": [
            ("monitoring_daemon", "Weekend low-traffic mode - all subsystems healthy"),
            ("statistics_collector", "Traffic 60% below weekday baseline - normal for weekend"),
            ("health_check", "System health check passed - CPU 8%, Memory 42%"),
            ("monitoring_daemon", "No alerts in last 24 hours - clean operation"),
            ("system_logger", "Weekend status nominal - no incidents"),
        ],
        "metrics": ("BaselineWeekend", ["cpu_pct", "mem_pct", "traffic_util_pct"]),
        "signals": ["ALL_CLEAR", "NORMAL_OPERATION"],
        "affected": [],
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
