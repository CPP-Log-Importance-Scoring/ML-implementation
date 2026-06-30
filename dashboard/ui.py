"""
dashboard/ui.py
================
Shared Streamlit UI helpers — theme CSS, time-window picker, status badges.
"""

from __future__ import annotations

import sys
import json
from datetime import date, datetime, time, timedelta
from pathlib import Path

import streamlit as st

_DASHBOARD_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT  = _DASHBOARD_DIR.parent
for _p in [str(_PROJECT_ROOT), str(_DASHBOARD_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)


# ---------------------------------------------------------------------------
# Filter Persistence
# ---------------------------------------------------------------------------

_FILTER_STORE_FILE = _PROJECT_ROOT / "storage" / "filters.json"

class _FilterEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date, time)):
            return obj.isoformat()
        return super().default(obj)

def persist_filters() -> None:
    data = {}
    for k, v in st.session_state.items():
        if k.startswith("_backing_"):
            data[k] = v
    try:
        _FILTER_STORE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_FILTER_STORE_FILE, "w") as f:
            json.dump(data, f, cls=_FilterEncoder)
    except Exception as e:
        print(f"Failed to save filters: {e}")

def load_filters() -> None:
    if "_filters_loaded" in st.session_state:
        return
    st.session_state["_filters_loaded"] = True
    if _FILTER_STORE_FILE.exists():
        try:
            with open(_FILTER_STORE_FILE, "r") as f:
                data = json.load(f)
            for k, v in data.items():
                if isinstance(v, dict):
                    for dk in ["start_date", "end_date"]:
                        if dk in v and isinstance(v[dk], str):
                            v[dk] = date.fromisoformat(v[dk])
                    for tk in ["start_time", "end_time"]:
                        if tk in v and isinstance(v[tk], str):
                            v[tk] = time.fromisoformat(v[tk])
                st.session_state[k] = v
        except Exception as e:
            print(f"Failed to load filters: {e}")

# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------

THEME_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&display=swap');

/* ── Reset & base ─────────────────────────── */
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    -webkit-font-smoothing: antialiased;
}

.block-container {
    padding-top: 1.6rem;
    padding-bottom: 2.5rem;
    max-width: 1440px;
}

/* ── Headings ─────────────────────────────── */
h1 {
    font-size: 1.6rem !important;
    font-weight: 700 !important;
    letter-spacing: -0.025em !important;
    color: #0f172a !important;
    line-height: 1.2 !important;
}
h2 {
    font-size: 1.15rem !important;
    font-weight: 600 !important;
    letter-spacing: -0.015em !important;
    color: #1e293b !important;
}
h3 {
    font-size: 0.95rem !important;
    font-weight: 600 !important;
    letter-spacing: -0.01em !important;
    color: #334155 !important;
}

/* ── Sidebar ──────────────────────────────── */
[data-testid="stSidebar"] {
    background: #f8fafc !important;
    border-right: 1px solid #e2e8f0 !important;
}

[data-testid="stSidebar"] .block-container {
    padding-top: 1rem;
}

/* Force all sidebar text dark */
[data-testid="stSidebar"] * {
    color: #0f172a !important;
}

/* Sidebar headings */
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
    font-size: 0.8rem !important;
    text-transform: uppercase;
    letter-spacing: 0.08em !important;
    color: #64748b !important;
    font-weight: 600 !important;
}

/* Navigation links */
/* Hide Streamlit's auto-generated page nav — every page renders the shared
   branded emoji navbar via render_sidebar_nav() instead. */
[data-testid="stSidebarNav"] {
    display: none !important;
}

[data-testid="stSidebarNav"] * {
    color: #0f172a !important;
}

[data-testid="stSidebarNavLink"] {
    border-radius: 8px !important;
    font-weight: 500 !important;
    font-size: 0.88rem !important;
    color: #0f172a !important;
}

[data-testid="stSidebarNavLink"]:hover {
    background: #e2e8f0 !important;
}

[data-testid="stSidebarNavLink"] a,
[data-testid="stSidebarNavLink"] span {
    color: #0f172a !important;
}

/* ── Metric tiles ─────────────────────────── */
[data-testid="stMetric"] {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 1rem 1.25rem;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04), 0 4px 8px rgba(0,0,0,0.03);
    transition: box-shadow 0.15s ease;
}
[data-testid="stMetric"]:hover {
    box-shadow: 0 4px 16px rgba(0,0,0,0.08);
}
[data-testid="stMetricLabel"] {
    font-size: 0.72rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: #64748b !important;
}
[data-testid="stMetricValue"] {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 1.65rem !important;
    font-weight: 600 !important;
    color: #0f172a !important;
}

/* ── Cards / bordered containers ─────────── */
div[data-testid="stVerticalBlockBorderWrapper"] {
    border-radius: 12px !important;
    border: 1px solid #e2e8f0 !important;
    background: #ffffff;
    transition: box-shadow 0.15s ease, border-color 0.15s ease;
    padding: 0.1rem 0;
}
div[data-testid="stVerticalBlockBorderWrapper"]:hover {
    box-shadow: 0 4px 20px rgba(0,0,0,0.07);
    border-color: #cbd5e1 !important;
}

/* ── Buttons ──────────────────────────────── */
.stButton {
    margin-top: 0.15rem;
    margin-bottom: 0.15rem;
}
.stButton > button {
    border-radius: 8px;
    font-size: 0.83rem;
    font-weight: 600;
    padding: 0.45rem 1.1rem;
    min-height: 2.45rem;
    border: 1px solid #e2e8f0;
    background: #ffffff;
    color: #374151;
    transition: all 0.15s ease;
    letter-spacing: 0.01em;
    width: 100%;
}
.stButton > button:hover {
    background: #f1f5f9;
    border-color: #94a3b8;
    color: #0f172a;
    transform: translateY(-1px);
    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
}
.stButton > button[kind="primary"] {
    background: #1d4ed8;
    border-color: #1d4ed8;
    color: white;
}
.stButton > button[kind="primary"]:hover {
    background: #1e40af;
    border-color: #1e40af;
}

/* ── Alerts ───────────────────────────────── */
.stAlert {
    border-radius: 10px;
    font-size: 0.88rem;
}

/* ── Dataframe ────────────────────────────── */
[data-testid="stDataFrame"] {
    border-radius: 10px;
    border: 1px solid #e2e8f0;
    overflow: hidden;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem;
}

/* ── Code ─────────────────────────────────── */
.stCode code, code {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.8rem !important;
}

/* ── Select / Input ───────────────────────── */
[data-testid="stSelectbox"] label,
[data-testid="stTextInput"] label,
[data-testid="stMultiSelect"] label {
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    color: #475569 !important;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}

/* ── Divider ──────────────────────────────── */
hr {
    border: none !important;
    border-top: 1px solid #e2e8f0 !important;
    margin: 0.75rem 0 !important;
}

/* ── Caption / small text ─────────────────── */
[data-testid="stCaptionContainer"] p,
.stCaption {
    color: #64748b !important;
    font-size: 0.8rem !important;
}

/* ── Expander ─────────────────────────────── */
[data-testid="stExpander"] {
    border: 1px solid #e2e8f0 !important;
    border-radius: 10px !important;
    background: #fafafa;
}
/* Header label was inheriting white-on-white; force a readable dark tone. */
[data-testid="stExpander"] summary,
[data-testid="stExpander"] summary p,
[data-testid="stExpander"] summary span,
[data-testid="stExpander"] details > summary * {
    color: #0f172a !important;
}

/* ── Spinner ──────────────────────────────── */
[data-testid="stSpinner"] {
    font-size: 0.85rem;
    color: #64748b;
}

/* ── Cross-system badge ───────────────────── */
.cross-system-badge {
    display: inline-block;
    background: #FEF3C7;
    color: #92400E;
    font-size: 10px;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 4px;
    border: 1px solid #F59E0B33;
    font-family: 'IBM Plex Mono', monospace;
    letter-spacing: 0.05em;
}

/* ── Page nav buttons in sidebar ─────────── */
[data-testid="stSidebarNavLink"] {
    border-radius: 8px !important;
    font-weight: 500 !important;
    font-size: 0.88rem !important;
}

/* ── Tab styling ──────────────────────────── */
[data-testid="stTabs"] [data-baseweb="tab"] {
    font-size: 0.85rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.01em;
}

/* ── Premium Custom Cards ─────────────────── */
.kpi-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 1.1rem;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04), 0 4px 8px rgba(0,0,0,0.03);
    transition: all 0.2s ease;
    text-align: left;
}
.kpi-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 16px rgba(0,0,0,0.08);
    border-color: #cbd5e1;
}
.kpi-title {
    font-size: 0.72rem;
    font-weight: 600;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.06em;
}
.kpi-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.6rem;
    font-weight: 700;
    color: #0f172a;
    margin-top: 4px;
}

.incident-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 1.25rem;
    margin-bottom: 0.75rem;
    transition: all 0.2s ease;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}
.incident-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 20px rgba(0,0,0,0.08);
    border-color: #cbd5e1;
}
.incident-card-critical {
    border-left: 5px solid #DC2626 !important;
}
.incident-card-medium {
    border-left: 5px solid #F59E0B !important;
}
.incident-card-low {
    border-left: 5px solid #22C55E !important;
}
.incident-card-ignore {
    border-left: 5px solid #94A3B8 !important;
}
</style>
"""


def apply_theme() -> None:
    """Inject the shared dashboard CSS theme."""
    load_filters()
    st.markdown(THEME_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

# (page path, emoji label) — the single source of truth for the sidebar nav.
_NAV_LINKS = [
    ("app.py",                   "Home"),
    ("pages/incident_feed.py",   "Incident Feed"),
    ("pages/incident_detail.py", "Incident Detail"),
    ("pages/host_health.py",     "Host Health"),
    ("pages/log_search.py",      "Log Search"),
    ("pages/upload_logs.py",     "Upload & Analyze"),
]


def render_sidebar_nav() -> None:
    """Render the shared branded sidebar navigation (emoji page links).

    Replaces Streamlit's auto-generated page nav (hidden via THEME_CSS) with a
    single branded navbar so every page shows identical, emoji-labelled links.
    Call once per page, right after ``apply_theme()`` and before any
    page-specific sidebar widgets.
    """
    with st.sidebar:
        st.markdown(
            """
            <div style='padding: 0.5rem 0 1rem 0;'>
              <div style='font-size:1.15rem; font-weight:700; color:#0f172a; letter-spacing:-0.02em;'>
                HPE CX Intelligence
              </div>
              <div style='font-size:0.7rem; color:#64748b; margin-top:2px; font-family:"IBM Plex Mono",monospace;'>
                Observability Platform
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.divider()
        for path, label in _NAV_LINKS:
            st.page_link(path, label=label)
        st.divider()


# ---------------------------------------------------------------------------
# Time window picker
# ---------------------------------------------------------------------------

def render_time_window(prefix: str = "time"):
    """
    Render start/end date+time pickers in the sidebar.
    Returns (start_dt, end_dt) as datetime objects.

    Values survive page navigation via a backing-store key.  Streamlit clears
    widget keys when the widget is not rendered on the current page, but never
    clears plain session_state keys set by application code.  The backing store
    is the source of truth; widget keys are seeded from it on each page entry.
    """
    now = datetime.now().replace(second=0, microsecond=0)
    default_start = now - timedelta(days=7)

    _bk = f"_backing_{prefix}_time"
    if _bk not in st.session_state:
        st.session_state[_bk] = {
            "start_date": default_start.date(),
            "start_time": time(0, 0),
            "end_date":   now.date(),
            "end_time":   time(23, 59),
        }
    _backed = st.session_state[_bk]

    _sd = f"{prefix}_start_date"
    _st = f"{prefix}_start_time"
    _ed = f"{prefix}_end_date"
    _et = f"{prefix}_end_time"
    if _sd not in st.session_state:
        st.session_state[_sd] = _backed["start_date"]
    if _st not in st.session_state:
        st.session_state[_st] = _backed["start_time"]
    if _ed not in st.session_state:
        st.session_state[_ed] = _backed["end_date"]
    if _et not in st.session_state:
        st.session_state[_et] = _backed["end_time"]

    start_date     = st.date_input("Start date",  value=st.session_state[_sd], key=_sd)
    start_time_val = st.time_input("Start time",  value=st.session_state[_st], key=_st, step=300)
    end_date       = st.date_input("End date",    value=st.session_state[_ed], key=_ed)
    end_time_val   = st.time_input("End time",    value=st.session_state[_et], key=_et, step=300)

    st.session_state[_bk] = {
        "start_date": start_date,
        "start_time": start_time_val,
        "end_date":   end_date,
        "end_time":   end_time_val,
    }
    persist_filters()

    start_dt = datetime.combine(start_date, start_time_val)
    end_dt   = datetime.combine(end_date, end_time_val)

    if end_dt < start_dt:
        st.error("End must be after start.")
        st.stop()

    st.caption(f"{start_dt:%d %b %Y %H:%M} → {end_dt:%d %b %Y %H:%M}")
    return start_dt, end_dt


# ---------------------------------------------------------------------------
# Quick time range preset
# ---------------------------------------------------------------------------

def render_time_range_select(key: str = "time_range") -> int:
    """
    A simpler time range selector using a radio group.
    Returns the number of hours for the selected range.
    """
    options = {
        "Last 1h": 1,
        "Last 6h": 6,
        "Last 24h": 24,
        "Last 7d": 168,
        "Last 30d": 720,
    }
    choice = st.radio(
        "Time range",
        list(options.keys()),
        index=2,
        horizontal=True,
        key=key,
    )
    return options[choice]


# ---------------------------------------------------------------------------
# Status indicator
# ---------------------------------------------------------------------------

def service_status_dot(healthy: bool, label: str) -> str:
    """Return an HTML status dot + label."""
    colour = "#22C55E" if healthy else "#EF4444"
    icon = "●"
    return (
        f"<span style='color:{colour}; font-size:11px; font-family:\"IBM Plex Mono\",monospace'>"
        f"{icon} {label}</span>"
    )
