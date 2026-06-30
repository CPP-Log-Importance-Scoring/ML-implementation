"""
dashboard/pages/upload_logs.py
==============================
Upload & Analyze page — allows operators to upload one or more raw .log/.txt
files, trigger the full analysis pipeline in the background, monitor live
progress, and view a result summary — no CLI required.

Workflow
--------
1. User uploads files via st.file_uploader.
2. On "Analyze", files are staged into data/raw/uploads/<batch_id>/.
3. pipeline.py is launched as a subprocess; --input-mode is auto-detected from
   the staged file contents (synthetic 7-section vs flat syslog) or chosen by the user.
4. Stdout/stderr are tailed from a per-batch pipeline.log file.
5. On success, a summary is rendered from data/processed/scored_logs_df.parquet.
"""

from __future__ import annotations

import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

# ── sys.path bootstrap ──────────────────────────────────────────────────────
_PAGES_DIR    = Path(__file__).resolve().parent
_DASHBOARD_DIR = _PAGES_DIR.parent
_PROJECT_ROOT  = _DASHBOARD_DIR.parent
for _p in [str(_PROJECT_ROOT), str(_DASHBOARD_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from ui import apply_theme, render_sidebar_nav  # noqa: E402  (must be after sys.path bootstrap)

from dashboard.data import db
from dashboard.archive_utils import stage_uploads

# ---------------------------------------------------------------------------
# Page config & theme
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Upload & Analyze — HPE CX Intelligence",
    page_icon="📤",
    layout="wide",
    initial_sidebar_state="expanded",
)
apply_theme()
render_sidebar_nav()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
UPLOADS_ROOT       = Path("data/raw/uploads")
SCORED_PARQUET     = Path("data/processed/scored_logs_df.parquet")
# Human-readable log content (message/host/timestamp/event) lives here; the
# scored parquet is scores-only, keyed by sequence_number. We join the two so
# the result view can show *what actually happened*, not just counts.
SESSIONIZED_PARQUET = Path("data/processed/sessionized_logs.parquet")
ANOMALY_PARQUET     = Path("data/processed/anomaly_df.parquet")

# Label severity order (worst-first) for sorting / display
LABEL_ORDER = ["critical", "medium", "low", "ignore"]
LABEL_COLORS = {
    "critical": "#ef4444",
    "medium":   "#f97316",
    "low":      "#eab308",
    "ignore":   "#64748b",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_batch_id() -> str:
    """Return a unique batch identifier: uploads/<UTC_TIMESTAMP>_<SHORT_UUID>."""
    ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:6]
    return f"uploads/{ts}_{uid}"


def _detect_input_mode(staged: list[Path]) -> str:
    """Resolve the pipeline --input-mode from the staged file contents.

    The 7-section vendor-neutral / synthetic format is a structured document
    marked by ``## SECTION`` headers; it must go through the synthetic loader.
    Anything else is treated as flat syslog. We cannot defer to pipeline.py's
    own ``auto`` mode here because the dashboard always passes a *directory*,
    and ``auto`` maps every directory to ``synthetic`` — which would misparse a
    directory of flat syslog files.
    """
    for path in staged:
        try:
            head = path.read_text(encoding="utf-8", errors="replace")[:8192]
        except Exception:
            continue
        if "## SECTION" in head:
            return "synthetic"
    return "syslog"


def _launch_pipeline(batch_dir: Path, dry_run: bool, input_mode: str) -> tuple[subprocess.Popen, Path]:
    """Launch pipeline.py as a subprocess and return (process, log_path)."""
    # Write the run log as a SIBLING of the batch dir, not inside it — otherwise
    # the directory parsers (synthetic loader / syslog run_directory both glob
    # ``<batch_dir>/*.log``) would ingest the pipeline's own log file.
    log_path = batch_dir.parent / f"{batch_dir.name}.pipeline.log"
    cmd = [
        sys.executable,
        str(_PROJECT_ROOT / "pipeline.py"),
        "--log-file", str(batch_dir),
        "--input-mode", input_mode,
    ]
    if dry_run:
        cmd.append("--dry-run")

    log_fh = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen(
        cmd,
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        cwd=str(_PROJECT_ROOT),
    )
    # Store the open file handle so it stays alive with the process.
    # We attach it to the process object for cleanup.
    proc._log_fh = log_fh  # type: ignore[attr-defined]
    return proc, log_path


def _tail_log(log_path: Path, last_n: int = 20) -> str:
    """Return the last N lines of a log file, or '' if it doesn't exist yet."""
    if not log_path.exists():
        return ""
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-last_n:])
    except Exception:
        return ""


def _load_results(parquet_path: Path) -> pd.DataFrame | None:
    """Read the scored-logs parquet, returning None on any error."""
    if not parquet_path.exists():
        return None
    try:
        return pd.read_parquet(parquet_path)
    except Exception:
        return None


def _update_feed_time_filter_once(df: pd.DataFrame | None) -> None:
    """Sync the Incident Feed time-window and filter backing stores to cover the
    uploaded batch.  Runs at most once per batch (guarded by job_batch_id) so
    that the user can freely adjust feed filters after visiting the page without
    them being reset on every re-render of this success view."""
    batch_id = st.session_state.get("job_batch_id")
    if batch_id and st.session_state.get("_feed_updated_batch") == batch_id:
        return

    _TIME_BK   = "_backing_global_time"
    _FILTER_BK = "_backing_feed_filters"

    feed_start = feed_end = None

    if df is not None and "timestamp" in df.columns:
        try:
            ts = pd.to_datetime(df["timestamp"], errors="coerce").dropna()
            if len(ts):
                feed_start = (ts.min() - pd.Timedelta(hours=1)).to_pydatetime()
                feed_end   = (ts.max() + pd.Timedelta(hours=1)).to_pydatetime()
        except Exception:
            pass

    if feed_start is None:
        from datetime import timedelta as _td
        feed_end   = datetime.now(timezone.utc).replace(tzinfo=None)
        feed_start = feed_end - _td(days=30)

    st.session_state[_TIME_BK] = {
        "start_date": feed_start.date(),
        "start_time": feed_start.time().replace(second=0, microsecond=0),
        "end_date":   feed_end.date(),
        "end_time":   feed_end.time().replace(second=0, microsecond=0),
    }
    for _k in ["global_start_date", "global_start_time", "global_end_date", "global_end_time"]:
        st.session_state.pop(_k, None)

    st.session_state[_FILTER_BK] = {
        "host_filter":       [],
        "severity_filter":   ["critical", "medium", "low"],
        "cross_system_only": False,
    }
    for _k in ["feed_host_filter", "feed_severity_filter", "feed_cross"]:
        st.session_state.pop(_k, None)

    st.session_state["_feed_updated_batch"] = batch_id

    from ui import persist_filters
    persist_filters()


# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------
def _init_state() -> None:
    defaults = {
        "job_batch_id":   None,   # str  — active batch id
        "job_batch_dir":  None,   # Path — staging directory
        "job_log_path":   None,   # Path — pipeline.log for this batch
        "job_proc":       None,   # subprocess.Popen
        "job_status":     None,   # "running" | "success" | "failed"
        "job_dry_run":    False,
        "job_input_mode": None,   # str  — resolved pipeline --input-mode
        "job_staged":     [],     # list[str] — staged file names
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


_init_state()


# ---------------------------------------------------------------------------
# Page header
# ---------------------------------------------------------------------------
st.markdown(
    """
    <div style='background: linear-gradient(135deg, #f8fbff 0%, #ffffff 55%, #eef6ff 100%);
                padding: 1.4rem 2rem; border-radius: 14px; margin-bottom: 1.2rem;
                box-shadow: 0 6px 20px rgba(15,23,42,0.06);
                border: 1px solid rgba(59,130,246,0.16);'>
      <div style='display:flex; align-items:center; gap:10px;'>
        <div>
          <div style='font-size:1.4rem; font-weight:800; color:#0f172a; letter-spacing:-0.02em;'>
            Upload & Analyze
          </div>
          <div style='color:#475569; font-size:0.88rem; margin-top:2px;'>
            Drag in one or more <code>.log</code> / <code>.txt</code> files — or a
            <code>.zip</code> / <code>.tar</code> / <code>.tar.gz</code> / <code>.tgz</code>
            archive — and run the full analysis pipeline. No CLI required.
          </div>
        </div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Upload section  (only shown when no job is running / completed)
# ---------------------------------------------------------------------------
job_running  = st.session_state.job_status == "running"
job_finished = st.session_state.job_status in ("success", "failed")

if not job_running and not job_finished:
    st.subheader("Select log files")

    uploaded_files = st.file_uploader(
        "Drop .log / .txt files or archives here (multiple allowed)",
        accept_multiple_files=True,
        type=["log", "txt", "zip", "tar", "gz", "tgz"],
        help="Individual logs or archives (.zip, .tar, .tar.gz, .tgz). "
             "Archives are extracted automatically; nested log files are "
             "discovered recursively.",
    )

    mode_choice = st.selectbox(
        "Parsing mode",
        ["Auto-detect", "Syslog (flat RFC 3164)", "Synthetic (7-section structured)"],
        help="Auto-detect inspects the uploaded files: structured 7-section logs "
             "(with '## SECTION' markers) use the synthetic loader; everything else "
             "uses the flat syslog sessionizer.",
    )

    dry_run_toggle = st.checkbox(
        "Dry run (skip Postgres write)",
        value=False,
        help="Real run is the default; results are written to Postgres. "
             "Enable this to test parsing without touching the database.",
    )

    analyze_disabled = not uploaded_files
    analyze_clicked  = st.button(
        "Analyze",
        type="primary",
        disabled=analyze_disabled,
        use_container_width=False,
    )

    if analyze_disabled:
        st.caption("Upload at least one file to enable analysis.")

    if analyze_clicked and uploaded_files:
        # ── Stage files (plain logs and/or archives) ─────────────────────
        batch_id  = _make_batch_id()
        batch_dir = UPLOADS_ROOT / Path(batch_id).name
        try:
            stage_result = stage_uploads(uploaded_files, batch_dir)
        except Exception as exc:
            st.error(f"Failed to stage uploaded files: {exc}")
            st.stop()

        staged = stage_result.staged_files

        # Surface per-archive warnings/errors without aborting the whole batch.
        for _err in stage_result.errors:
            st.error(_err)
        for _warn in stage_result.warnings:
            st.warning(_warn)

        if not staged:
            st.error(
                "No analysable .log/.txt files were found in your upload. "
                "Check that archives contain log files and are not "
                "password-protected or corrupted."
            )
            st.stop()

        if stage_result.archive_count:
            st.info(
                f"Extracted {stage_result.extracted_log_count:,} log file(s) "
                f"from {stage_result.archive_count} archive(s) in "
                f"{stage_result.elapsed_seconds:.2f}s."
                + (f" Rejected {stage_result.rejected_unsafe_count} unsafe "
                   "archive entr(ies)." if stage_result.rejected_unsafe_count else "")
            )

        # ── Resolve parsing mode ──────────────────────────────────────────
        _mode_map = {
            "Auto-detect": None,  # resolved from file contents below
            "Syslog (flat RFC 3164)": "syslog",
            "Synthetic (7-section structured)": "synthetic",
        }
        input_mode = _mode_map[mode_choice] or _detect_input_mode(staged)

        # ── Launch pipeline ───────────────────────────────────────────────
        try:
            proc, log_path = _launch_pipeline(batch_dir, dry_run=dry_run_toggle, input_mode=input_mode)
        except Exception as exc:
            st.error(f"Failed to launch pipeline: {exc}")
            st.stop()

        # ── Persist state ─────────────────────────────────────────────────
        st.session_state.job_batch_id   = batch_id
        st.session_state.job_batch_dir  = batch_dir
        st.session_state.job_log_path   = log_path
        st.session_state.job_proc       = proc
        st.session_state.job_status     = "running"
        st.session_state.job_dry_run    = dry_run_toggle
        st.session_state.job_input_mode = input_mode
        st.session_state.job_staged     = [f.name for f in staged]

        st.rerun()


# ---------------------------------------------------------------------------
# Progress section  (running)
# ---------------------------------------------------------------------------
if st.session_state.job_status == "running":
    proc: subprocess.Popen = st.session_state.job_proc

    # Poll process
    return_code = proc.poll()

    st.subheader("Pipeline running…")

    info_col, meta_col = st.columns([2, 1])
    with info_col:
        _staged_names = st.session_state.job_staged or []
        if len(_staged_names) > 8:
            _staged_disp = ", ".join(_staged_names[:8]) + f" … (+{len(_staged_names) - 8} more)"
        else:
            _staged_disp = ", ".join(_staged_names)
        st.info(
            f"**Batch:** `{st.session_state.job_batch_id}`  \n"
            f"**Files staged:** {len(_staged_names)} file(s) — {_staged_disp}  \n"
            f"**Parsing mode:** `{st.session_state.job_input_mode}`  \n"
            f"**Dry run:** {'Yes' if st.session_state.job_dry_run else 'No'}"
        )
    with meta_col:
        st.metric("Status", "Running")

    log_text = _tail_log(st.session_state.job_log_path, last_n=25)
    with st.expander("Pipeline log (live tail)", expanded=True):
        st.code(log_text or "(waiting for output…)", language="bash")

    if return_code is None:
        # Still running — auto-refresh every 2 s
        time.sleep(2)
        st.rerun()
    else:
        # Process has exited
        try:
            proc._log_fh.close()  # type: ignore[attr-defined]
        except Exception:
            pass

        if return_code == 0:
            st.session_state.job_status = "success"
        else:
            st.session_state.job_status = "failed"
        st.rerun()


# ---------------------------------------------------------------------------
# Results / error section  (finished)
# ---------------------------------------------------------------------------
if st.session_state.job_status in ("success", "failed"):
    status_ok = st.session_state.job_status == "success"

    # ── Header ──────────────────────────────────────────────────────────────
    if status_ok:
        st.success(
            f"Pipeline completed successfully — batch `{st.session_state.job_batch_id}`"
        )
    else:
        st.error(
            f"Pipeline failed — batch `{st.session_state.job_batch_id}`"
        )

    # ── Final log dump ───────────────────────────────────────────────────────
    with st.expander("Full pipeline log", expanded=not status_ok):
        log_text = _tail_log(st.session_state.job_log_path, last_n=200)
        st.code(log_text or "(log not found)", language="bash")

    # ── Result summary (only on success) ────────────────────────────────────
    if status_ok:
        df = _load_results(SCORED_PARQUET)

        if df is None:
            st.warning(
                "Pipeline reported success but the result parquet was not found at "
                f"`{SCORED_PARQUET}`. Check the log for details."
            )
        else:
            # ── Attach human-readable log fields ─────────────────────────
            # scored_logs_df is scores-only; join the sessionized logs (which
            # carry message/host/timestamp/event_type) on sequence_number so
            # the operator can read the actual flagged log lines.
            logs_df = _load_results(SESSIONIZED_PARQUET)
            if (
                logs_df is not None
                and "sequence_number" in df.columns
                and "sequence_number" in logs_df.columns
            ):
                join_cols = [
                    c for c in
                    ["sequence_number", "timestamp", "host", "service",
                     "log_level", "event_type", "event_action", "message",
                     "source_file"]
                    if c in logs_df.columns
                ]
                df = df.merge(logs_df[join_cols], on="sequence_number", how="left")

            st.markdown("---")
            st.subheader("Result Summary")

            # ── Overview metrics ─────────────────────────────────────────
            total_rows      = len(df)
            # Count anomalies from the current batch's parquet — not the
            # global DB table, which accumulates across all prior runs.
            _anomaly_df = _load_results(ANOMALY_PARQUET)
            if _anomaly_df is not None and "is_anomaly" in _anomaly_df.columns:
                total_anomalies = int(_anomaly_df["is_anomaly"].sum())
            else:
                total_anomalies = db.get_anomaly_count()
            label_col_exists = "label" in df.columns

            # Critical-only UI: surface criticals plus mediums that belong to a
            # formed incident (non-empty correlation_id). Internal scoring and
            # labels are untouched — low / ignore / standalone-medium rows simply
            # aren't shown here. Revert this block to restore the full breakdown.
            def _in_incident(series: pd.Series) -> pd.Series:
                s = series.astype(str).str.strip().str.lower()
                return series.notna() & ~s.isin(["", "nan", "none"])

            if label_col_exists:
                _is_crit = df["label"] == "critical"
                _is_inc_medium = (
                    (df["label"] == "medium") & _in_incident(df["correlation_id"])
                    if "correlation_id" in df.columns
                    else pd.Series(False, index=df.index)
                )
                _surfaced_mask = _is_crit | _is_inc_medium
                n_critical   = int(_is_crit.sum())
                n_inc_medium = int(_is_inc_medium.sum())
            else:
                _surfaced_mask = None
                n_critical = n_inc_medium = 0

            m1, m2, m3 = st.columns(3)
            m1.metric("Total rows processed", f"{total_rows:,}")
            m2.metric("Anomalies detected",   total_anomalies if isinstance(total_anomalies, str) else f"{total_anomalies:,}")
            m3.metric("Critical Logs", f"{n_critical:,}" if label_col_exists else "N/A")

            # ── Volume stats ─────────────────────────────────────────────
            _surfaced    = total_anomalies if isinstance(total_anomalies, int) else 0
            _reduction   = (1.0 - _surfaced / total_rows) * 100 if total_rows > 0 else 0.0
            _signal_rate = (_surfaced / total_rows) * 100 if total_rows > 0 else 0.0
            _reduction_color = "#15803d" if _reduction >= 80 else ("#b45309" if _reduction >= 50 else "#dc2626")

            st.markdown(
                f"""
                <div style='display:flex; gap:1rem; margin-top:1rem; flex-wrap:wrap;'>
                  <div style='flex:1; min-width:160px; background:#f8fafc; border:1px solid #e2e8f0;
                              border-radius:10px; padding:0.85rem 1rem;'>
                    <div style='font-size:0.72rem; text-transform:uppercase; letter-spacing:0.1em;
                                color:#64748b; font-weight:600;'>Noise Reduction</div>
                    <div style='font-size:1.45rem; font-weight:800; color:{_reduction_color};
                                font-family:"IBM Plex Mono",monospace; margin-top:2px;'>{_reduction:.1f}%</div>
                    <div style='font-size:0.75rem; color:#94a3b8; margin-top:2px;'>of volume suppressed by ML</div>
                  </div>
                  <div style='flex:1; min-width:160px; background:#f8fafc; border:1px solid #e2e8f0;
                              border-radius:10px; padding:0.85rem 1rem;'>
                    <div style='font-size:0.72rem; text-transform:uppercase; letter-spacing:0.1em;
                                color:#64748b; font-weight:600;'>Signal Rate</div>
                    <div style='font-size:1.45rem; font-weight:800; color:#7c3aed;
                                font-family:"IBM Plex Mono",monospace; margin-top:2px;'>{_signal_rate:.1f}%</div>
                    <div style='font-size:0.75rem; color:#94a3b8; margin-top:2px;'>of stream flagged as anomalous</div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # ── Surfaced severity (critical-only view) ───────────────────
            if label_col_exists:
                st.markdown("#### Surfaced Severity")
                _suppressed = total_rows - int(_surfaced_mask.sum())
                _tiles = [
                    ("critical",    n_critical,   "critical"),
                    ("in-incident", n_inc_medium, "medium"),
                ]
                sev_cols = st.columns(len(_tiles))
                for col, (caption, count, ckey) in zip(sev_cols, _tiles):
                    color = LABEL_COLORS.get(ckey, "#64748b")
                    col.markdown(
                        f"""
                        <div style='background:{color}18; border:1px solid {color}44;
                                    border-radius:10px; padding:0.7rem 1rem; text-align:center;'>
                          <div style='font-size:1.6rem; font-weight:800; color:{color};'>{count:,}</div>
                          <div style='font-size:0.78rem; color:#475569; text-transform:uppercase;
                                      letter-spacing:0.12em; margin-top:2px;'>{caption}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                st.caption(
                    f"{_suppressed:,} lower-severity line(s) hidden from this view"
                )

            # ── Critical & medium logs — what actually happened ──────────
            score_col = next(
                (c for c in ("final_score", "combined_score") if c in df.columns),
                None,
            )

            if label_col_exists:
                flagged = df[_surfaced_mask].copy()
                if score_col:
                    flagged = flagged.sort_values(score_col, ascending=False)
                flagged = flagged.reset_index(drop=True)

                st.markdown("#### Critical Logs")

                if flagged.empty:
                    st.info("No critical or in-incident logs were flagged in this batch.")
                else:
                    # Colour-code the label column
                    def _style_label(val: str) -> str:
                        color = LABEL_COLORS.get(str(val).lower(), "#475569")
                        return f"color: {color}; font-weight: 600;"

                    display_cols = [
                        c for c in
                        ["timestamp", "host", "log_level", "event_type",
                         "label", score_col, "message", "correlation_id"]
                        if c and c in flagged.columns
                    ]

                    table_df = flagged[display_cols]
                    styled = table_df.style
                    if "label" in display_cols:
                        # pandas >=2.1 renamed Styler.applymap -> Styler.map
                        _elementwise = getattr(styled, "map", None) or styled.applymap
                        styled = _elementwise(_style_label, subset=["label"])
                    if score_col and score_col in display_cols:
                        styled = styled.format({score_col: "{:.4f}"})

                    st.dataframe(styled, use_container_width=True, hide_index=True)
                    st.caption(
                        f"Showing all {len(flagged):,} flagged log line(s). "
                        "Use Incident Feed / Log Search for full triage."
                    )

                    # Detailed drill-down for the top critical lines
                    crit = flagged[flagged["label"] == "critical"]
                    if not crit.empty and "message" in crit.columns:
                        st.markdown("##### Critical line details")
                        for _, row in crit.head(10).iterrows():
                            host = row.get("host", "—")
                            ts   = row.get("timestamp", "—")
                            etype = row.get("event_type", "")
                            header = f"{ts}  ·  {host}" + (f"  ·  {etype}" if etype else "")
                            with st.expander(header):
                                st.write(row.get("message", "(no message)"))
                                meta = {
                                    k: row[k] for k in
                                    ("service", "log_level", "event_action",
                                     score_col, "correlation_id", "source_file")
                                    if k and k in row.index and pd.notna(row[k])
                                }
                                if meta:
                                    st.json(meta, expanded=False)

    # ── Navigate to Incident Feed ────────────────────────────────────────────
    if status_ok and not st.session_state.get("job_dry_run", False):
        _update_feed_time_filter_once(df)
        _col_feed, _ = st.columns([2, 5])
        with _col_feed:
            if st.button(
                "View incidents in Incident Feed →",
                type="primary",
                use_container_width=True,
                key="nav_to_feed",
            ):
                st.switch_page("pages/incident_feed.py")

    # ── Restart button ───────────────────────────────────────────────────────
    st.markdown("---")
    if st.button("Upload more files / start new analysis", type="secondary"):
        # Clear job state so the upload form reappears
        for key in [
            "job_batch_id", "job_batch_dir", "job_log_path",
            "job_proc", "job_status", "job_dry_run", "job_input_mode", "job_staged",
        ]:
            st.session_state[key] = None if key != "job_staged" else []
        st.rerun()