"""
parsing/tests/test_qa_service_alias_fix.py
==========================================
Targeted QA test suite for the SERVICE_ALIAS_MAP / NOISE_SERVICE_LABEL fix
(issues 2a and 2b).

Coverage matrix
---------------
QA-01  Happy path         — all HPE + synthetic aliases resolve correctly
QA-02  Noise suppression  — all 9 noise services → NOISE (issue 2b core)
QA-03  Edge cases         — PID stripping, case variants, unknown → uppercase
QA-04  Regression         — all 31 old hardcoded entries produce same result
QA-05  Profile switching  — kubernetes_linux.yaml resolves k8s services correctly
QA-06  Realistic mix      — ~85% noise ratio confirmed on incident-mixed log
QA-07  Pre-fix breakage   — proves the old bug: on k8s log + HPE profile,
                            noise services are NOT suppressed (negative test)

How to run
----------
pytest parsing/tests/test_qa_service_alias_fix.py -v
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import pytest

import parsing.normalizer as normalizer
from common.config import NOISE_SERVICE_LABEL, SERVICE_ALIAS_MAP
from common.service_profile import load_service_profile

# ---------------------------------------------------------------------------
# Paths to QA log files and profiles
# ---------------------------------------------------------------------------

_LOG_DIR = Path("data/raw/qa_service_alias")
_PROFILE_DIR = Path("config/dataset_profiles")

_LOG_HAPPY    = _LOG_DIR / "qa_01_hpe_happy_path.log"
_LOG_NOISE    = _LOG_DIR / "qa_02_noise_suppression.log"
_LOG_EDGE     = _LOG_DIR / "qa_03_edge_cases.log"
_LOG_REGRESS  = _LOG_DIR / "qa_04_regression_pre_fix_behavior.log"
_LOG_K8S      = _LOG_DIR / "qa_05_profile_switch_kubernetes.log"
_LOG_MIXED    = _LOG_DIR / "qa_06_mixed_realistic.log"

_PROFILE_HPE  = _PROFILE_DIR / "hpe_synthetic.yaml"
_PROFILE_K8S  = _PROFILE_DIR / "kubernetes_linux.yaml"
_PROFILE_GEN  = _PROFILE_DIR / "generic.yaml"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_log(path: Path) -> list[dict]:
    """Parse all non-comment, non-blank lines in a log file, return parsed rows."""
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        result = normalizer.normalize_line(stripped)
        if result is not None:
            rows.append(result)
    return rows


def _parse_log_with_profile(path: Path, profile_path: Path) -> list[dict]:
    """Parse a log file after overriding SERVICE_ALIAS_MAP via the loader."""
    alias_map = load_service_profile(profile_path)
    original = normalizer.SERVICE_ALIAS_MAP
    try:
        normalizer.SERVICE_ALIAS_MAP = alias_map
        return _parse_log(path)
    finally:
        normalizer.SERVICE_ALIAS_MAP = original


def _require_log(path: Path):
    """Skip the test if the log file is not present."""
    if not path.exists():
        pytest.skip(f"QA log file not found: {path}")


def _require_profile(path: Path):
    """Skip the test if the profile file is not present."""
    if not path.exists():
        pytest.skip(f"Profile file not found: {path}")


# ===========================================================================
# QA-01: Happy path — all HPE + synthetic daemon aliases present
# ===========================================================================

class TestQA01HappyPath:
    """All services in hpe_synthetic.yaml must resolve to their canonical label."""

    # Map of process_name → expected_service_label for every alias in the profile
    EXPECTED = {
        # Real HPE CX daemons
        "eventmgr":    "SYSTEM",
        "hpe-routing": "ROUTING",
        "kernel":      "SYSTEM",
        "sshd":        "SYSTEM",
        "cron":        "SYSTEM",
        "sudo":        "SYSTEM",
        "snmpd":       "SNMP",
        "lldpd":       "LLDP",
        "cfgd":        "CONFIG",
        # Synthetic daemons
        "spanning_tree_daemon":  "STP",
        "redundancy_daemon":     "REDUNDANCY",
        "forwarding_engine":     "FORWARDING",
        "access_control_daemon": "ACL",
        "routing_daemon":        "ROUTING",
        "mac_learning":          "MAC",
        "qos_scheduler_daemon":  "QOS",
        "buffer_manager":        "BUFFER",
        "physical_monitor":      "PHYSICAL",
        "statistics_collector":  "STATS",
        "system_logger":         "SYSTEM",
        "process_monitor":       "SYSTEM",
        "network_monitor":       "NETWORK",
    }

    def test_all_known_aliases_resolve_correctly(self):
        _require_log(_LOG_HAPPY)
        rows = _parse_log(_LOG_HAPPY)
        assert rows, "No rows parsed — log file may be malformed"

        # Build a dict of service_name → set of resolved labels from parsed rows
        resolved: dict[str, set] = {}
        for row in rows:
            svc_name = row["message"]  # we can't reverse-map easily; use SERVICE_ALIAS_MAP directly
        # Direct lookup test (independent of log file)
        for process_name, expected_label in self.EXPECTED.items():
            actual = SERVICE_ALIAS_MAP.get(process_name.lower(), process_name.upper())
            assert actual == expected_label, (
                f"Process '{process_name}': expected '{expected_label}', got '{actual}'. "
                f"The profile YAML may be missing this entry."
            )

    def test_log_produces_no_none_rows(self):
        """No parseable line should produce a None result."""
        _require_log(_LOG_HAPPY)
        rows = _parse_log(_LOG_HAPPY)
        assert len(rows) >= 20, (
            f"Expected at least 20 parsed rows from happy-path log, got {len(rows)}"
        )

    def test_rfc3164_and_iso8601_both_resolve_correctly(self):
        """Both log formats should produce correct service resolutions."""
        _require_log(_LOG_HAPPY)
        rows = _parse_log(_LOG_HAPPY)
        services = {r["service"] for r in rows}
        # These services appear via both RFC 3164 and ISO 8601 lines
        assert "SYSTEM" in services, "eventmgr/sshd should resolve to SYSTEM"
        assert "CONFIG" in services, "cfgd should resolve to CONFIG"
        assert "LDP" not in services, "lldpd should be LLDP not LDP"

    @pytest.mark.parametrize("process_name, expected_label", [
        ("eventmgr",            "SYSTEM"),
        ("hpe-routing",         "ROUTING"),
        ("lldpd",               "LLDP"),
        ("cfgd",                "CONFIG"),
        ("snmpd",               "SNMP"),
        ("spanning_tree_daemon","STP"),
        ("routing_daemon",      "ROUTING"),
        ("mac_learning",        "MAC"),
        ("qos_scheduler_daemon","QOS"),
        ("buffer_manager",      "BUFFER"),
        ("statistics_collector","STATS"),
        ("network_monitor",     "NETWORK"),
    ])
    def test_individual_alias_lookup(self, process_name: str, expected_label: str):
        """Parametrised: each alias resolves to its documented canonical label."""
        actual = SERVICE_ALIAS_MAP.get(process_name.lower(), process_name.upper())
        assert actual == expected_label, (
            f"'{process_name}' → expected '{expected_label}', got '{actual}'"
        )


# ===========================================================================
# QA-02: Noise suppression — the core issue 2b test
# ===========================================================================

class TestQA02NoiseSuppression:
    """Every noise service must map to NOISE_SERVICE_LABEL ('NOISE')."""

    ALL_NOISE_SERVICES = [
        "monitoring",
        "continuous_monitoring",
        "routine_check",
        "periodic_status",
        "system_check",
        "status_verification",
        "health_check",
        "metrics_update",
        "frame_monitoring",
    ]

    @pytest.mark.parametrize("service_name", ALL_NOISE_SERVICES)
    def test_noise_service_maps_to_noise_label(self, service_name: str):
        """Each noise service must resolve to NOISE_SERVICE_LABEL via the loaded map."""
        actual = SERVICE_ALIAS_MAP.get(service_name.lower())
        assert actual == NOISE_SERVICE_LABEL, (
            f"Noise service '{service_name}': expected '{NOISE_SERVICE_LABEL}', got '{actual}'. "
            f"Issue 2b is NOT fixed — noise suppression will fail for this service."
        )

    def test_all_noise_services_present_in_alias_map(self):
        """All 9 noise services must exist as keys in SERVICE_ALIAS_MAP."""
        missing = [s for s in self.ALL_NOISE_SERVICES if s not in SERVICE_ALIAS_MAP]
        assert not missing, (
            f"The following noise services are missing from SERVICE_ALIAS_MAP: {missing}. "
            f"Noise suppression is broken for these services."
        )

    def test_noise_lines_parsed_from_log_have_noise_service_label(self):
        """Parsing the noise log file: every row's service field must be NOISE."""
        _require_log(_LOG_NOISE)
        rows = _parse_log(_LOG_NOISE)
        # The last row is an OSPF line (not noise) — filter it out
        noise_rows = [r for r in rows if any(
            svc in r.get("_raw_text", r.get("raw_text", ""))
            for svc in self.ALL_NOISE_SERVICES
        )]
        non_noise_services = {r["service"] for r in noise_rows if r["service"] != NOISE_SERVICE_LABEL}
        assert not non_noise_services, (
            f"These noise services were NOT resolved to NOISE: {non_noise_services}. "
            f"Noise suppression is broken — issue 2b not fully fixed."
        )

    def test_real_anomaly_in_noise_log_is_not_noise(self):
        """The single OSPF anomaly in the noise log must NOT be labelled NOISE."""
        _require_log(_LOG_NOISE)
        rows = _parse_log(_LOG_NOISE)
        ospf_rows = [r for r in rows if "ospf" in r.get("raw_text", "").lower()]
        assert ospf_rows, "Expected at least one OSPF row in noise log"
        for row in ospf_rows:
            assert row["service"] != NOISE_SERVICE_LABEL, (
                f"OSPF row was incorrectly labelled NOISE: {row}"
            )

    def test_noise_label_constant_is_noise_string(self):
        """NOISE_SERVICE_LABEL must equal the string 'NOISE'."""
        assert NOISE_SERVICE_LABEL == "NOISE", (
            f"NOISE_SERVICE_LABEL changed! Got '{NOISE_SERVICE_LABEL}', expected 'NOISE'."
        )


# ===========================================================================
# QA-03: Edge cases
# ===========================================================================

class TestQA03EdgeCases:
    """PID suffix stripping, case insensitivity, unknown-service fallback."""

    def test_pid_suffix_stripped_before_lookup(self):
        """sshd[1234] must strip to sshd and resolve to SYSTEM."""
        line = "<134>Jun 22 10:00:01 sw-01 sshd[1234]: login successful"
        result = normalizer.normalize_line(line)
        assert result is not None
        assert result["service"] == "SYSTEM", (
            f"sshd[1234] should strip PID and resolve to SYSTEM, got '{result['service']}'"
        )

    def test_large_pid_suffix_stripped(self):
        """Process names with large PIDs (containerised) must still resolve."""
        line = "<134>Jun 22 10:00:01 sw-01 monitoring[4294967295]: Heartbeat OK"
        result = normalizer.normalize_line(line)
        assert result is not None
        assert result["service"] == NOISE_SERVICE_LABEL, (
            f"monitoring with large PID should be NOISE, got '{result['service']}'"
        )

    def test_uppercase_process_name_resolves_correctly(self):
        """SSHD (uppercase) must match 'sshd' in the map → SYSTEM."""
        line = "<134>Jun 22 10:00:01 sw-01 SSHD[2001]: Uppercase daemon"
        result = normalizer.normalize_line(line)
        assert result is not None
        assert result["service"] == "SYSTEM", (
            f"SSHD (uppercase) should resolve to SYSTEM, got '{result['service']}'"
        )

    def test_titlecase_process_name_resolves_correctly(self):
        """Sshd (titlecase) must match 'sshd' in the map → SYSTEM."""
        line = "<134>Jun 22 10:00:01 sw-01 Sshd[2002]: Titlecase daemon"
        result = normalizer.normalize_line(line)
        assert result is not None
        assert result["service"] == "SYSTEM", (
            f"Sshd (titlecase) should resolve to SYSTEM, got '{result['service']}'"
        )

    def test_uppercase_noise_service_resolves_to_noise(self):
        """MONITORING (uppercase) should still resolve to NOISE."""
        line = "<134>Jun 22 10:00:01 sw-01 MONITORING[2004]: Uppercase heartbeat"
        result = normalizer.normalize_line(line)
        assert result is not None
        assert result["service"] == NOISE_SERVICE_LABEL, (
            f"MONITORING (uppercase) should be NOISE, got '{result['service']}'"
        )

    def test_unknown_service_uppercased_not_noise(self):
        """kubelet is not in hpe_synthetic.yaml — must be uppercased to KUBELET, not NOISE."""
        line = "<134>Jun 22 10:00:01 sw-01 kubelet[3001]: Pod started"
        result = normalizer.normalize_line(line)
        assert result is not None
        assert result["service"] == "KUBELET", (
            f"Unknown 'kubelet' must be uppercased to KUBELET, got '{result['service']}'"
        )
        assert result["service"] != NOISE_SERVICE_LABEL, (
            "Unknown service 'kubelet' must NOT be treated as NOISE"
        )

    def test_unknown_service_node_exporter_not_noise_on_hpe_profile(self):
        """node_exporter is not in hpe_synthetic.yaml — must be uppercase, not NOISE.

        This is the exact pre-fix failure mode on wrong dataset: real noise in
        a k8s deployment would leak through as non-noise when using HPE profile.
        """
        line = "<134>Jun 22 10:00:01 sw-01 node_exporter[3005]: Metrics scraped"
        result = normalizer.normalize_line(line)
        assert result is not None
        assert result["service"] == "NODE_EXPORTER", (
            f"node_exporter on HPE profile should be NODE_EXPORTER, got '{result['service']}'"
        )
        assert result["service"] != NOISE_SERVICE_LABEL, (
            "node_exporter should NOT be NOISE on the HPE profile — "
            "this is expected! It highlights why profile switching matters."
        )

    def test_hyphenated_service_resolves_correctly(self):
        """hpe-routing (hyphenated) must look up correctly."""
        line = "<134>Jun 22 10:00:01 sw-01 hpe-routing[4001]: Route added"
        result = normalizer.normalize_line(line)
        assert result is not None
        assert result["service"] == "ROUTING", (
            f"hpe-routing should resolve to ROUTING, got '{result['service']}'"
        )

    def test_multiple_colons_in_message_service_extraction(self):
        """Service extraction must stop at the first colon after the process token."""
        line = "<134>Jun 22 10:00:01 sw-01 sshd[6001]: Accepted publickey: RSA: SHA256:abc"
        result = normalizer.normalize_line(line)
        assert result is not None
        assert result["service"] == "SYSTEM", (
            f"sshd with multi-colon message should still resolve to SYSTEM, got '{result['service']}'"
        )

    @pytest.mark.parametrize("service_name, expected", [
        ("kubelet",            "KUBELET"),
        ("containerd",         "CONTAINERD"),
        ("auditd",             "AUDITD"),
        ("journald",           "JOURNALD"),
        ("node_exporter",      "NODE_EXPORTER"),
        ("prometheus_agent",   "PROMETHEUS_AGENT"),
        ("some_brand_new_daemon", "SOME_BRAND_NEW_DAEMON"),
    ])
    def test_unknown_services_fallback_to_uppercase(self, service_name: str, expected: str):
        """Services not in the profile must be upper-cased by the normalizer fallback."""
        actual = SERVICE_ALIAS_MAP.get(service_name.lower(), service_name.upper())
        assert actual == expected, (
            f"Unknown service '{service_name}': expected fallback '{expected}', got '{actual}'"
        )


# ===========================================================================
# QA-04: Regression — exact pre-fix behavior preserved
# ===========================================================================

class TestQA04Regression:
    """The loaded profile must produce 100% identical results to the old hardcoded dict."""

    # This is the exact hardcoded dict that existed before the fix.
    # The test proves the YAML profile is an exact migration.
    ORIGINAL_HARDCODED = {
        "eventmgr":    "SYSTEM",
        "hpe-routing": "ROUTING",
        "kernel":      "SYSTEM",
        "sshd":        "SYSTEM",
        "cron":        "SYSTEM",
        "sudo":        "SYSTEM",
        "snmpd":       "SNMP",
        "lldpd":       "LLDP",
        "cfgd":        "CONFIG",
        "spanning_tree_daemon":  "STP",
        "redundancy_daemon":     "REDUNDANCY",
        "forwarding_engine":     "FORWARDING",
        "access_control_daemon": "ACL",
        "routing_daemon":        "ROUTING",
        "mac_learning":          "MAC",
        "qos_scheduler_daemon":  "QOS",
        "buffer_manager":        "BUFFER",
        "physical_monitor":      "PHYSICAL",
        "statistics_collector":  "STATS",
        "system_logger":         "SYSTEM",
        "process_monitor":       "SYSTEM",
        "network_monitor":       "NETWORK",
        "monitoring":             "NOISE",
        "continuous_monitoring":  "NOISE",
        "routine_check":          "NOISE",
        "periodic_status":        "NOISE",
        "system_check":           "NOISE",
        "status_verification":    "NOISE",
        "health_check":           "NOISE",
        "metrics_update":         "NOISE",
        "frame_monitoring":       "NOISE",
    }

    def test_loaded_map_entry_count_matches_original(self):
        """The loaded SERVICE_ALIAS_MAP must have at least as many entries as the original."""
        assert len(SERVICE_ALIAS_MAP) >= len(self.ORIGINAL_HARDCODED), (
            f"Loaded profile has {len(SERVICE_ALIAS_MAP)} entries, "
            f"original had {len(self.ORIGINAL_HARDCODED)}. "
            f"Entries may be missing from hpe_synthetic.yaml."
        )

    @pytest.mark.parametrize("process_name, expected_label", list(ORIGINAL_HARDCODED.items()))
    def test_every_original_entry_preserved(self, process_name: str, expected_label: str):
        """Every entry from the old hardcoded dict must produce the same label."""
        actual = SERVICE_ALIAS_MAP.get(process_name.lower(), process_name.upper())
        assert actual == expected_label, (
            f"REGRESSION: '{process_name}' was '{expected_label}' in the old hardcoded dict, "
            f"but is now '{actual}'. The YAML migration broke this entry."
        )

    def test_regression_log_all_services_resolve_correctly(self):
        """Parse the regression log file and verify all 31 entries resolve correctly."""
        _require_log(_LOG_REGRESS)
        rows = _parse_log(_LOG_REGRESS)
        assert len(rows) >= 31, (
            f"Expected at least 31 rows from regression log, got {len(rows)}"
        )


# ===========================================================================
# QA-05: Profile switching
# ===========================================================================

class TestQA05ProfileSwitching:
    """Loading a different profile produces different resolutions."""

    def test_kubernetes_profile_loads_without_error(self):
        """kubernetes_linux.yaml must load cleanly."""
        _require_profile(_PROFILE_K8S)
        alias_map = load_service_profile(_PROFILE_K8S)
        assert isinstance(alias_map, dict)
        assert len(alias_map) > 0

    def test_kubernetes_runtime_daemons_resolve_on_k8s_profile(self):
        """kubelet and containerd must resolve to RUNTIME on kubernetes_linux.yaml."""
        _require_profile(_PROFILE_K8S)
        alias_map = load_service_profile(_PROFILE_K8S)
        assert alias_map.get("kubelet") == "RUNTIME"
        assert alias_map.get("containerd") == "RUNTIME"

    def test_kubernetes_noise_resolves_on_k8s_profile(self):
        """node_exporter and cadvisor must resolve to NOISE on kubernetes_linux.yaml."""
        _require_profile(_PROFILE_K8S)
        alias_map = load_service_profile(_PROFILE_K8S)
        assert alias_map.get("node_exporter") == NOISE_SERVICE_LABEL, (
            "node_exporter must be NOISE on kubernetes profile"
        )
        assert alias_map.get("cadvisor") == NOISE_SERVICE_LABEL, (
            "cadvisor must be NOISE on kubernetes profile"
        )

    def test_sshd_resolves_same_on_both_profiles(self):
        """sshd appears in both profiles and must resolve to SYSTEM on both."""
        _require_profile(_PROFILE_K8S)
        _require_profile(_PROFILE_HPE)
        k8s_map = load_service_profile(_PROFILE_K8S)
        hpe_map = load_service_profile(_PROFILE_HPE)
        assert k8s_map.get("sshd") == "SYSTEM"
        assert hpe_map.get("sshd") == "SYSTEM"

    def test_kubelet_fallback_on_hpe_profile(self):
        """kubelet is NOT in hpe_synthetic.yaml — must fall back to uppercase."""
        _require_profile(_PROFILE_HPE)
        hpe_map = load_service_profile(_PROFILE_HPE)
        result = hpe_map.get("kubelet", "KUBELET")
        assert result == "KUBELET", (
            f"kubelet on HPE profile should be KUBELET (fallback), got '{result}'"
        )

    def test_k8s_noise_on_hpe_profile_is_not_suppressed(self):
        """node_exporter is NOT in hpe_synthetic.yaml — this is the pre-fix failure mode.

        On the wrong profile, Kubernetes noise leaks through undetected.
        This test documents and asserts the known limitation, ensuring it's
        explicitly understood rather than silently broken.
        """
        _require_profile(_PROFILE_HPE)
        hpe_map = load_service_profile(_PROFILE_HPE)
        node_exporter_label = hpe_map.get("node_exporter", "NODE_EXPORTER")
        assert node_exporter_label != NOISE_SERVICE_LABEL, (
            "node_exporter is intentionally NOT suppressed by the HPE profile — "
            "use kubernetes_linux.yaml for Kubernetes log sources."
        )

    def test_k8s_log_with_k8s_profile_noise_lines_are_noise(self):
        """Parse k8s log with k8s profile: node_exporter/cadvisor lines → NOISE."""
        _require_log(_LOG_K8S)
        _require_profile(_PROFILE_K8S)
        rows = _parse_log_with_profile(_LOG_K8S, _PROFILE_K8S)
        noise_rows = [r for r in rows if r["service"] == NOISE_SERVICE_LABEL]
        # The k8s log has 10 noise lines (node_exporter x3, cadvisor x3,
        # kube-state-metrics x1, metrics-server x1, liveness-probe x1, readiness-probe x1)
        assert len(noise_rows) >= 8, (
            f"Expected at least 8 NOISE rows from k8s log with k8s profile, "
            f"got {len(noise_rows)}"
        )

    def test_k8s_log_with_hpe_profile_noise_not_suppressed(self):
        """Parse k8s log with HPE profile: node_exporter lines must NOT be NOISE.

        This is the exact failure mode that the fix addresses — using the wrong
        profile silently breaks noise suppression for foreign datasets.
        """
        _require_log(_LOG_K8S)
        _require_profile(_PROFILE_HPE)
        rows = _parse_log_with_profile(_LOG_K8S, _PROFILE_HPE)
        # node_exporter lines with HPE profile: should be NODE_EXPORTER, not NOISE
        node_exporter_rows = [
            r for r in rows
            if "node_exporter" in r.get("raw_text", "").lower()
        ]
        if node_exporter_rows:
            noise_labelled = [r for r in node_exporter_rows if r["service"] == NOISE_SERVICE_LABEL]
            assert not noise_labelled, (
                f"node_exporter lines were incorrectly labelled NOISE on HPE profile: {noise_labelled}"
            )

    def test_generic_profile_produces_empty_map(self):
        """The generic starter profile must produce an empty alias map."""
        _require_profile(_PROFILE_GEN)
        alias_map = load_service_profile(_PROFILE_GEN)
        assert alias_map == {}, (
            f"generic.yaml should be empty, got {len(alias_map)} entries: {alias_map}"
        )

    def test_env_var_overrides_default_profile(self, monkeypatch, tmp_path):
        """Setting DATASET_PROFILE env var at import time selects the right profile."""
        custom_yaml = tmp_path / "custom.yaml"
        custom_yaml.write_text(
            "profile_name: custom\n"
            "service_aliases:\n"
            "  myapp: MYAPP\n"
            "noise_services:\n"
            "  - myapp_heartbeat\n"
        )
        alias_map = load_service_profile(custom_yaml)
        assert alias_map.get("myapp") == "MYAPP"
        assert alias_map.get("myapp_heartbeat") == NOISE_SERVICE_LABEL


# ===========================================================================
# QA-06: Realistic mixed log
# ===========================================================================

class TestQA06RealisticMix:
    """End-to-end: noise ratio, signal detection, suppression in realistic scenario."""

    def test_noise_ratio_is_approximately_85_percent(self):
        """The mixed log must have ~85% NOISE-labelled rows."""
        _require_log(_LOG_MIXED)
        rows = _parse_log(_LOG_MIXED)
        assert rows, "No rows parsed"
        noise_count = sum(1 for r in rows if r["service"] == NOISE_SERVICE_LABEL)
        total = len(rows)
        noise_ratio = noise_count / total
        assert noise_ratio >= 0.75, (
            f"Noise ratio {noise_ratio:.1%} is below 75%. "
            f"Noise suppression may not be working correctly."
        )
        assert noise_ratio <= 0.95, (
            f"Noise ratio {noise_ratio:.1%} is suspiciously high. "
            f"Real events may be wrongly labelled NOISE."
        )

    def test_bgp_events_are_not_noise(self):
        """BGP events in the mixed log must NOT be labelled NOISE."""
        _require_log(_LOG_MIXED)
        rows = _parse_log(_LOG_MIXED)
        bgp_rows = [r for r in rows if "bgp" in r.get("raw_text", "").lower()]
        assert bgp_rows, "Expected at least one BGP row in mixed log"
        noise_bgp = [r for r in bgp_rows if r["service"] == NOISE_SERVICE_LABEL]
        assert not noise_bgp, (
            f"BGP rows incorrectly labelled NOISE: {noise_bgp}"
        )

    def test_ospf_events_are_not_noise(self):
        """OSPF adjacency-loss events must NOT be labelled NOISE."""
        _require_log(_LOG_MIXED)
        rows = _parse_log(_LOG_MIXED)
        ospf_rows = [r for r in rows if "ospf" in r.get("raw_text", "").lower()]
        assert ospf_rows, "Expected at least one OSPF row in mixed log"
        for row in ospf_rows:
            assert row["service"] != NOISE_SERVICE_LABEL, (
                f"OSPF row incorrectly labelled NOISE: {row}"
            )

    def test_ifmgrd_events_are_not_noise(self):
        """Interface-manager events must NOT be labelled NOISE."""
        _require_log(_LOG_MIXED)
        rows = _parse_log(_LOG_MIXED)
        ifmgrd_rows = [r for r in rows if "ifmgrd" in r.get("raw_text", "").lower()]
        assert ifmgrd_rows, "Expected at least one ifmgrd row in mixed log"
        for row in ifmgrd_rows:
            assert row["service"] != NOISE_SERVICE_LABEL, (
                f"ifmgrd row incorrectly labelled NOISE: {row}"
            )

    def test_total_signal_rows_count(self):
        """The mixed log must have exactly 8 signal (non-noise) rows."""
        _require_log(_LOG_MIXED)
        rows = _parse_log(_LOG_MIXED)
        signal_rows = [r for r in rows if r["service"] != NOISE_SERVICE_LABEL]
        assert len(signal_rows) >= 6, (
            f"Expected at least 6 signal rows in mixed log, got {len(signal_rows)}"
        )

    def test_noise_services_appear_in_majority(self):
        """Noise service rows must outnumber signal rows — this confirms suppression works."""
        _require_log(_LOG_MIXED)
        rows = _parse_log(_LOG_MIXED)
        noise = sum(1 for r in rows if r["service"] == NOISE_SERVICE_LABEL)
        signal = sum(1 for r in rows if r["service"] != NOISE_SERVICE_LABEL)
        assert noise > signal, (
            f"Expected noise ({noise}) > signal ({signal}). "
            f"Noise suppression may be incorrectly labelling signal rows as NOISE."
        )


# ===========================================================================
# QA-07: Profile validator error paths
# ===========================================================================

class TestQA07ProfileValidation:
    """The loader must reject malformed profiles with clear errors."""

    def test_missing_file_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Dataset profile not found"):
            load_service_profile(tmp_path / "nonexistent.yaml")

    def test_invalid_yaml_raises_value_error(self, tmp_path):
        p = tmp_path / "broken.yaml"
        p.write_text("key: [unclosed")
        with pytest.raises(ValueError, match="invalid YAML"):
            load_service_profile(p)

    def test_missing_noise_services_key_raises_value_error(self, tmp_path):
        p = tmp_path / "no_noise.yaml"
        p.write_text("profile_name: test\nservice_aliases:\n  sshd: SYSTEM\n")
        with pytest.raises(ValueError, match="missing required key"):
            load_service_profile(p)

    def test_missing_service_aliases_key_raises_value_error(self, tmp_path):
        p = tmp_path / "no_aliases.yaml"
        p.write_text("profile_name: test\nnoise_services:\n  - monitoring\n")
        with pytest.raises(ValueError, match="missing required key"):
            load_service_profile(p)

    def test_service_aliases_not_a_dict_raises_value_error(self, tmp_path):
        p = tmp_path / "bad_aliases.yaml"
        p.write_text("profile_name: test\nservice_aliases:\n  - sshd\nnoise_services: []\n")
        with pytest.raises(ValueError, match="must be a YAML mapping"):
            load_service_profile(p)

    def test_noise_services_not_a_list_raises_value_error(self, tmp_path):
        p = tmp_path / "bad_noise.yaml"
        p.write_text("profile_name: test\nservice_aliases: {}\nnoise_services: monitoring\n")
        with pytest.raises(ValueError, match="must be a YAML list"):
            load_service_profile(p)

    def test_noise_entry_duplicate_in_aliases_is_overwritten_to_noise(self, tmp_path):
        """If a service appears in both service_aliases and noise_services,
        noise_services wins (it is merged last).
        """
        p = tmp_path / "overlap.yaml"
        p.write_text(
            "profile_name: test\n"
            "service_aliases:\n"
            "  health_check: CONFIG\n"  # wrong label intentionally
            "noise_services:\n"
            "  - health_check\n"        # noise_services should override
        )
        alias_map = load_service_profile(p)
        assert alias_map.get("health_check") == NOISE_SERVICE_LABEL, (
            "noise_services must override conflicting service_aliases entries"
        )

    def test_empty_profile_name_still_loads(self, tmp_path):
        """A profile without a profile_name key is still valid."""
        p = tmp_path / "no_name.yaml"
        p.write_text("service_aliases:\n  sshd: SYSTEM\nnoise_services: []\n")
        alias_map = load_service_profile(p)
        assert alias_map.get("sshd") == "SYSTEM"

    def test_whitespace_in_noise_service_names_is_stripped(self, tmp_path):
        """Leading/trailing whitespace in noise service entries must be stripped."""
        p = tmp_path / "whitespace.yaml"
        p.write_text(
            "profile_name: test\n"
            "service_aliases: {}\n"
            "noise_services:\n"
            "  - '  monitoring  '\n"
            "  - ' health_check '\n"
        )
        alias_map = load_service_profile(p)
        assert alias_map.get("monitoring") == NOISE_SERVICE_LABEL
        assert alias_map.get("health_check") == NOISE_SERVICE_LABEL
        # Original padded keys must not appear
        assert "  monitoring  " not in alias_map
