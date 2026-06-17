"""
dashboard/components/severity_badge.py
=======================================
Reusable severity badge component for the HPE CX dashboard.

BUG FIX: The original used single-quoted style attributes ( style='...' )
with double-quoted font-family inside ( font-family: "IBM Plex Mono" ).
When embedded inside the outer incident card f-string which also uses
single-quoted class attributes, Streamlit's HTML sanitizer mis-parsed
the quote nesting and rendered raw HTML for LOW and IGNORE labels.

Fix: style attribute now uses double quotes, font-family has no inner
quotes. Self-contained regardless of outer HTML context.
"""

from __future__ import annotations

import streamlit as st

# Palette: critical=red, medium=amber/orange, low=green, ignore=slate
_COLOURS: dict[str, tuple[str, str]] = {
    "critical": ("#DC2626", "#FEF2F2"),   # (text colour, pill background)
    "medium":   ("#B45309", "#FFFBEB"),
    "low":      ("#15803D", "#F0FDF4"),
    "ignore":   ("#475569", "#F1F5F9"),
}

_DEFAULT = ("#475569", "#F1F5F9")


def severity_badge(label: str, size: str = "sm") -> str:
    """
    Return an HTML string for an inline severity badge.
    Suitable for use with st.markdown(..., unsafe_allow_html=True).

    Uses double-quoted style attribute so it embeds safely inside any
    outer HTML context regardless of surrounding quote style.
    """
    label_lower = (label or "ignore").lower()
    fg, bg = _COLOURS.get(label_lower, _DEFAULT)
    font_size = "11px" if size == "sm" else "13px"
    padding   = "3px 9px" if size == "sm" else "4px 12px"

    # FIX: style uses double quotes; font-family has no inner quotes.
    # Previously: style='... font-family: "IBM Plex Mono" ...'
    # The inner double quotes broke HTML parsing when the badge was
    # embedded inside outer f-strings that also had single-quoted attrs.
    return (
        "<span style=\""
        + f"background:{bg}; color:{fg}; "
        + f"font-size:{font_size}; font-weight:700; "
        + f"padding:{padding}; border-radius:20px; "
        + f"letter-spacing:0.06em; border:1px solid {fg}33; "
        + "font-family:IBM Plex Mono,monospace;"
        + f"\">{label_lower.upper()}</span>"
    )


def render_severity_badge(label: str, size: str = "sm") -> None:
    """Render a severity badge directly into Streamlit."""
    st.markdown(severity_badge(label, size), unsafe_allow_html=True)


def severity_dot(label: str) -> str:
    """Return a small coloured circle span — for compact lists."""
    label_lower = (label or "ignore").lower()
    fg, _ = _COLOURS.get(label_lower, _DEFAULT)
    return (
        f"<span style=\"display:inline-block; width:8px; height:8px; "
        f"border-radius:50%; background:{fg}; margin-right:6px;\"></span>"
    )