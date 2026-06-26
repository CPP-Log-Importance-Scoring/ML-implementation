"""
dashboard/pages/incident_feed.py
==================================
Page 1 — Incident Feed
"""

import sys
from pathlib import Path

_DASHBOARD_DIR = Path(__file__).resolve().parents[1]
_PROJECT_ROOT  = _DASHBOARD_DIR.parent
for _p in [str(_PROJECT_ROOT), str(_DASHBOARD_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import streamlit as st
from data import db
from ui import apply_theme, render_sidebar_nav, render_time_window
from components.severity_badge import severity_badge

st.set_page_config(
    page_title="Incident Feed · HPE CX",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)
apply_theme()
render_sidebar_nav()

_FEED_FILTER_BK = "_backing_feed_filters"
if _FEED_FILTER_BK not in st.session_state:
    st.session_state[_FEED_FILTER_BK] = {
        "host_filter":       [],
        "severity_filter":   ["critical", "medium", "low"],
        "cross_system_only": False,
    }
_fbk = st.session_state[_FEED_FILTER_BK]

if "feed_host_filter" not in st.session_state:
    st.session_state["feed_host_filter"] = list(_fbk["host_filter"])
if "feed_severity_filter" not in st.session_state:
    st.session_state["feed_severity_filter"] = list(_fbk["severity_filter"])
if "feed_cross" not in st.session_state:
    st.session_state["feed_cross"] = _fbk["cross_system_only"]

with st.sidebar:
    st.markdown(
        "<div style=\"font-size:0.75rem; font-weight:700; text-transform:uppercase; "
        "letter-spacing:0.08em; color:#64748b; padding-bottom:0.4rem;\">Filters</div>",
        unsafe_allow_html=True,
    )
    start_dt, end_dt = render_time_window("feed")
    st.markdown("---")
    all_hosts = db.get_host_list()
    host_filter       = st.multiselect("Host", options=all_hosts, default=[], placeholder="All hosts", key="feed_host_filter")
    severity_filter   = st.multiselect("Severity", options=["critical", "medium", "low", "ignore"], default=["critical", "medium", "low"], key="feed_severity_filter")
    cross_system_only = st.toggle("Cross-system only", value=False, key="feed_cross")
    st.markdown("---")
    st.caption("Showing up to 200 most recent incidents.")

    st.session_state[_FEED_FILTER_BK] = {
        "host_filter":       host_filter,
        "severity_filter":   severity_filter,
        "cross_system_only": cross_system_only,
    }

with st.spinner("Loading incidents…"):
    incidents = db.get_incidents(
        host=host_filter[0] if len(host_filter) == 1 else None,
        severity=severity_filter if severity_filter else None,
        start_time=start_dt,
        end_time=end_dt,
        cross_system_only=cross_system_only,
    )
    if len(host_filter) > 1:
        incidents = [i for i in incidents if i.get("host") in host_filter]

st.markdown("<h1>Incident Feed</h1>", unsafe_allow_html=True)

total          = len(incidents)
critical_count = sum(1 for i in incidents if (i.get("label") or "").lower() == "critical")
cross_count    = sum(1 for i in incidents if i.get("is_cross_system"))
affected_hosts: dict[str, int] = {}
for inc in incidents:
    h = inc.get("host", "")
    affected_hosts[h] = affected_hosts.get(h, 0) + 1
most_affected = max(affected_hosts, key=affected_hosts.get) if affected_hosts else "—"

st.markdown(
    "<div style=\"display:flex; gap:1rem; width:100%; margin-bottom:1.5rem; flex-wrap:wrap;\">"
    + "<div class=\"kpi-card\" style=\"flex:1; min-width:200px;\"><div class=\"kpi-title\">Total Incidents</div><div class=\"kpi-value\">" + str(total) + "</div></div>"
    + "<div class=\"kpi-card\" style=\"flex:1; min-width:200px;\"><div class=\"kpi-title\" style=\"color:#dc2626;\">Critical Incidents</div><div class=\"kpi-value\" style=\"color:#dc2626;\">" + str(critical_count) + "</div></div>"
    + "<div class=\"kpi-card\" style=\"flex:1; min-width:200px;\"><div class=\"kpi-title\">Cross-System</div><div class=\"kpi-value\">" + str(cross_count) + "</div></div>"
    + "<div class=\"kpi-card\" style=\"flex:1; min-width:200px;\"><div class=\"kpi-title\">Most Affected Host</div><div class=\"kpi-value\" style=\"font-size:1.25rem; font-weight:700; padding-top:6px;\">" + str(most_affected) + "</div></div>"
    + "</div>",
    unsafe_allow_html=True,
)

if not incidents:
    st.markdown(
        "<div style=\"background:#f8fafc; border:1px dashed #cbd5e1; border-radius:12px; padding:3rem 2rem; text-align:center; margin-top:1rem;\">"
        "<div style=\"font-weight:600; color:#334155; font-size:1rem;\">No incidents found</div>"
        "<div style=\"color:#64748b; font-size:0.85rem; margin-top:0.4rem;\">Adjust your filters or run the scoring pipeline.</div>"
        "</div>",
        unsafe_allow_html=True,
    )
    st.stop()

st.markdown(
    "<div style=\"font-size:0.78rem; font-weight:600; color:#64748b; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:0.8rem;\">"
    + f"Showing {total} incident{'s' if total != 1 else ''}</div>",
    unsafe_allow_html=True,
)#T

_LABEL_ORDER = {"critical": 0, "medium": 1, "low": 2, "ignore": 3}
incidents_sorted = sorted(incidents, key=lambda i: _LABEL_ORDER.get((i.get("label") or "ignore").lower(), 99))

import pandas as pd

for incident in incidents_sorted:
    cid        = incident.get("correlation_id", "—")
    label      = (incident.get("label") or "ignore").lower()
    host       = incident.get("host", "—")
    start      = incident.get("start_time")
    end        = incident.get("end_time")
    log_count  = incident.get("log_count", 0)
    duration   = incident.get("duration", 0)
    is_cross   = incident.get("is_cross_system", False)
    final_score = float(incident.get("final_score") or 0.0)
    rc_conf    = float(incident.get("root_cause_confidence") or 0.0)

    try:
        start_str = pd.to_datetime(start).strftime("%d %b %Y, %H:%M") if start else "—"
    except Exception:
        start_str = str(start)[:16] if start else "—"
    try:
        end_str = pd.to_datetime(end).strftime("%H:%M") if end else "—"
    except Exception:
        end_str = str(end)[:16] if end else "—"

    if duration and duration > 0:
        if duration < 60:       dur_str = f"{duration}s"
        elif duration < 3600:   dur_str = f"{duration // 60}m {duration % 60}s"
        else:                   dur_str = f"{duration // 3600}h {(duration % 3600) // 60}m"
    else:
        dur_str = "—"

    # FIX: sanitize summary so any quotes/apostrophes in LLM output
    # cannot break the surrounding HTML block
    raw_summary = db.get_summary(cid) or ""
    summary_preview = (raw_summary[:180] + ("…" if len(raw_summary) > 180 else "")) if raw_summary else "No summary cached for this incident."
    # escape any HTML-breaking characters from LLM output
    summary_preview = (summary_preview
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;"))

    border_color = {"critical": "#DC2626", "medium": "#F59E0B", "low": "#22C55E", "ignore": "#94A3B8"}.get(label, "#94A3B8")
    cross_badge  = "<span style=\"background:#fef3c7; color:#92400e; font-size:9px; padding:2px 6px; border-radius:4px; font-weight:700; margin-left:6px;\">⚠ CROSS-SYS</span>" if is_cross else ""

    with st.container():
        col_card, col_btn = st.columns([8.5, 1.5], vertical_alignment="top")
        with col_card:
            # FIX: entire card built via string concatenation — no f-string
            # wrapping user-sourced content (summary, host, cid) inside
            # attribute values. All dynamic values go into element text nodes only.
            card_html = (
                "<div style=\"border-left:4px solid " + border_color + "; background:#ffffff; border-radius:8px; padding:0.9rem 1rem; margin-bottom:0.5rem; border:1px solid #e2e8f0; border-left:4px solid " + border_color + ";\">"

                # Row 1: badge + id + cross tag + duration/events
                "<div style=\"display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:6px;\">"
                "<div style=\"display:flex; align-items:center; gap:8px; flex-wrap:wrap;\">"
                + severity_badge(label)
                + "<span style=\"font-family:IBM Plex Mono,monospace; font-weight:700; font-size:0.95rem; color:#0f172a;\">" + str(cid) + "</span>"
                + cross_badge
                + "</div>"
                "<div style=\"font-family:IBM Plex Mono,monospace; font-size:0.78rem; color:#64748b;\">"
                + dur_str + " &nbsp;&middot;&nbsp; " + f"{log_count:,}" + " events"
                "</div>"
                "</div>"

                # Row 2: timestamp + host
                "<div style=\"margin-top:6px; font-size:0.8rem; color:#475569; font-family:IBM Plex Mono,monospace;\">"
                + start_str + " &rarr; " + end_str
                + " &nbsp;&middot;&nbsp; Host: <span style=\"font-weight:600; color:#0f172a;\">" + str(host) + "</span>"
                "</div>"

                # Row 3: summary preview — content is already HTML-escaped above
                "<div style=\"margin-top:8px; font-size:0.83rem; color:#334155; line-height:1.5; font-style:italic; border-left:3px solid #e2e8f0; padding-left:8px;\">"
                + summary_preview
                + "</div>"

                # Row 4: scores
                "<div style=\"margin-top:10px; display:flex; gap:20px; align-items:center; flex-wrap:wrap;\">"
                "<div style=\"font-size:0.75rem; color:#64748b; font-weight:600; text-transform:uppercase; letter-spacing:0.04em;\">"
                "Final Score: <span style=\"color:#0f172a; font-family:IBM Plex Mono,monospace; font-size:0.8rem; font-weight:700;\">" + f"{final_score:.3f}" + "</span>"
                "</div>"
                "<div style=\"font-size:0.75rem; color:#64748b; font-weight:600; text-transform:uppercase; letter-spacing:0.04em;\">"
                "Root Cause Confidence: <span style=\"color:#0f172a; font-family:IBM Plex Mono,monospace; font-size:0.8rem; font-weight:700;\">" + f"{rc_conf:.0%}" + "</span>"
                "</div>"
                "</div>"

                "</div>"
            )
            st.markdown(card_html, unsafe_allow_html=True)

        with col_btn:
            st.markdown("<div style=\"min-height:0.35rem;\"></div>", unsafe_allow_html=True)
            if st.button("View details →", key=f"view_{cid}", use_container_width=True, type="primary"):
                st.session_state["selected_incident"] = cid
                st.switch_page("pages/incident_detail.py")