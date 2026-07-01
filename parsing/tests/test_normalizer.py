from __future__ import annotations

import textwrap
from datetime import datetime
from pathlib import Path

import pytest

import parsing.normalizer as normalizer
from common.service_profile import load_service_profile


class FrozenDateTime(datetime):
    """datetime subclass with a fixed current date for timestamp tests."""

    @classmethod
    def now(cls):
        return cls(2026, 1, 1, 0, 0, 0)

    @classmethod
    def strptime(cls, value, fmt):
        return datetime.strptime(value, fmt)


# ---------------------------------------------------------------------------
# Existing timestamp tests (unchanged)
# ---------------------------------------------------------------------------

def test_parse_timestamp_uses_previous_year_for_future_bsd_date(monkeypatch):
    monkeypatch.setattr(normalizer, "datetime", FrozenDateTime)

    ts = normalizer._parse_timestamp("Dec 31 23:59:59")

    assert ts is not None
    assert ts.year == 2025
    assert ts == datetime(2025, 12, 31, 23, 59, 59)


def test_normalize_line_keeps_bsd_timestamp_in_previous_year(monkeypatch):
    monkeypatch.setattr(normalizer, "datetime", FrozenDateTime)

    parsed = normalizer.normalize_line("Dec 31 23:59:59 sw-01 OSPF: adjacency lost")

    assert parsed is not None
    assert parsed["timestamp"].year == 2025
    assert parsed["timestamp"] == datetime(2025, 12, 31, 23, 59, 59)


# ---------------------------------------------------------------------------
# New: service profile loader tests (issues 2a & 2b)
# ---------------------------------------------------------------------------

def _write_profile(tmp_path: Path, content: str) -> Path:
    """Helper: write YAML content to a temp file and return its path."""
    p = tmp_path / "test_profile.yaml"
    p.write_text(textwrap.dedent(content))
    return p


def test_load_service_profile_returns_alias_entries(tmp_path):
    """service_aliases entries are present in the returned dict."""
    p = _write_profile(tmp_path, """
        profile_name: test
        service_aliases:
          sshd: SYSTEM
          lldpd: LLDP
        noise_services: []
    """)
    alias_map = load_service_profile(p)

    assert alias_map["sshd"] == "SYSTEM"
    assert alias_map["lldpd"] == "LLDP"


def test_load_service_profile_noise_services_mapped_to_noise(tmp_path):
    """Entries in noise_services are merged into the map as NOISE (issue 2b fix)."""
    p = _write_profile(tmp_path, """
        profile_name: test
        service_aliases: {}
        noise_services:
          - monitoring
          - health_check
          - node_exporter
    """)
    alias_map = load_service_profile(p)

    assert alias_map["monitoring"] == "NOISE"
    assert alias_map["health_check"] == "NOISE"
    assert alias_map["node_exporter"] == "NOISE"


def test_load_service_profile_keys_are_lowercased(tmp_path):
    """Profile keys are normalised to lowercase for case-insensitive lookup."""
    p = _write_profile(tmp_path, """
        profile_name: test
        service_aliases:
          SSHD: SYSTEM
          Kernel: SYSTEM
        noise_services: []
    """)
    alias_map = load_service_profile(p)

    assert "sshd" in alias_map
    assert "kernel" in alias_map
    # Original mixed-case keys must not be present
    assert "SSHD" not in alias_map


def test_load_service_profile_values_are_uppercased(tmp_path):
    """Profile values are normalised to uppercase."""
    p = _write_profile(tmp_path, """
        profile_name: test
        service_aliases:
          cfgd: config
        noise_services: []
    """)
    alias_map = load_service_profile(p)

    assert alias_map["cfgd"] == "CONFIG"


def test_load_service_profile_unknown_service_not_in_map(tmp_path):
    """A process name absent from the profile is NOT in the map (caller uses fallback)."""
    p = _write_profile(tmp_path, """
        profile_name: test
        service_aliases:
          sshd: SYSTEM
        noise_services: []
    """)
    alias_map = load_service_profile(p)

    # 'kubelet' is not in this profile — caller does .get("kubelet", "KUBELET")
    assert "kubelet" not in alias_map


def test_load_service_profile_generic_profile_is_empty(tmp_path):
    """The generic starter profile produces an empty alias map."""
    generic_path = Path("config/dataset_profiles/generic.yaml")
    if not generic_path.exists():
        pytest.skip("generic.yaml not found — skipping integration check")

    alias_map = load_service_profile(generic_path)

    assert alias_map == {}, (
        "generic.yaml should produce an empty map so it is safe as a blank slate"
    )


def test_load_service_profile_hpe_synthetic_noise_entries_are_noise():
    """The default hpe_synthetic profile maps all heartbeat services to NOISE."""
    hpe_path = Path("config/dataset_profiles/hpe_synthetic.yaml")
    if not hpe_path.exists():
        pytest.skip("hpe_synthetic.yaml not found — skipping integration check")

    alias_map = load_service_profile(hpe_path)

    expected_noise = [
        "monitoring", "continuous_monitoring", "routine_check",
        "periodic_status", "system_check", "status_verification",
        "health_check", "metrics_update", "frame_monitoring",
    ]
    for svc in expected_noise:
        assert alias_map.get(svc) == "NOISE", (
            f"Expected '{svc}' → 'NOISE' in hpe_synthetic profile, "
            f"got '{alias_map.get(svc)}'"
        )


def test_load_service_profile_raises_on_missing_file(tmp_path):
    """FileNotFoundError is raised with a helpful message when the file is absent."""
    with pytest.raises(FileNotFoundError, match="Dataset profile not found"):
        load_service_profile(tmp_path / "nonexistent.yaml")


def test_load_service_profile_raises_on_invalid_yaml(tmp_path):
    """ValueError is raised when the YAML cannot be parsed."""
    p = tmp_path / "bad.yaml"
    p.write_text("key: [unclosed")
    with pytest.raises(ValueError, match="invalid YAML"):
        load_service_profile(p)


def test_load_service_profile_raises_on_missing_required_keys(tmp_path):
    """ValueError is raised when required top-level keys are absent."""
    p = _write_profile(tmp_path, """
        profile_name: test
        service_aliases:
          sshd: SYSTEM
        # noise_services is missing
    """)
    with pytest.raises(ValueError, match="missing required key"):
        load_service_profile(p)


def test_normalize_line_resolves_service_via_profile(monkeypatch):
    """normalizer._extract_service() still resolves known daemons correctly.

    This verifies that the normalizer's SERVICE_ALIAS_MAP (now loaded from
    hpe_synthetic.yaml) still maps 'sshd' → 'SYSTEM' as before.
    """
    monkeypatch.setattr(normalizer, "datetime", FrozenDateTime)

    parsed = normalizer.normalize_line(
        "2024-01-15 10:23:45 sw-01 sshd: login successful"
    )

    assert parsed is not None
    assert parsed["service"] == "SYSTEM", (
        "sshd should still resolve to SYSTEM via the hpe_synthetic profile"
    )


def test_normalize_line_unknown_service_uppercased(monkeypatch):
    """A process name not in the profile is upper-cased (fallback preserved)."""
    monkeypatch.setattr(normalizer, "datetime", FrozenDateTime)

    parsed = normalizer.normalize_line(
        "2024-01-15 10:23:45 sw-01 kubelet: pod started"
    )

    assert parsed is not None
    assert parsed["service"] == "KUBELET", (
        "Unknown process 'kubelet' should be upper-cased to 'KUBELET'"
    )

