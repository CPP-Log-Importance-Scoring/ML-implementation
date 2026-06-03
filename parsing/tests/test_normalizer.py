from __future__ import annotations

from datetime import datetime

import parsing.normalizer as normalizer


class FrozenDateTime(datetime):
    """datetime subclass with a fixed current date for timestamp tests."""

    @classmethod
    def now(cls):
        return cls(2026, 1, 1, 0, 0, 0)

    @classmethod
    def strptime(cls, value, fmt):
        return datetime.strptime(value, fmt)


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
