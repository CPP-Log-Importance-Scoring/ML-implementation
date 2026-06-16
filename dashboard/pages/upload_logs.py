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
3. pipeline.py is launched as a subprocess with --input-mode syslog.
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

from ui import apply_theme  # noqa: E402  (must be after sys.path bootstrap)

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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
UPLOADS_ROOT   = Path("data/raw/uploads")
SCORED_PARQUET = Path("data/processed/scored_logs_df.parquet")

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


def _stage_files(uploaded_files, batch_dir: Path) -> list[Path]:
    """Write uploaded UploadedFile objects to the batch staging directory."""
    batch_dir.mkdir(parents=True, exist_ok=True)
    staged: list[Path] = []
    for uf in uploaded_files:
        dest = batch_dir / uf.name
        dest.write_bytes(uf.getvalue())
        staged.append(dest)
    return staged


def _launch_pipeline(batch_dir: Path, dry_run: bool) -> tuple[subprocess.Popen, Path]:
    """Launch pipeline.py as a subprocess and return (process, log_path)."""
    log_path = batch_dir / "pipeline.log"
    cmd = [
        sys.executable,
        str(_PROJECT_ROOT / "pipeline.py"),
        "--log-file", str(batch_dir),
        "--input-mode", "syslog",
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
        "job_staged":     [],     # list[str] — staged file names
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


_init_state()


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown(
        """
        <div style='padding: 0.5rem 0 1rem 0;'>
          <div style='font-size:1.15rem; font-weight:700; color:#0f172a; letter-spacing:-0.02em;'>
            ⚡ HPE CX Intelligence
          </div>
          <div style='font-size:0.7rem; color:#64748b; margin-top:2px;'>
            Observability Platform
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.divider()
    st.page_link("app.py",                   label="🏠 Home")
    st.page_link("pages/incident_feed.py",   label="📋 Incident Feed")
    st.page_link("pages/incident_detail.py", label="🔍 Incident Detail")
    st.page_link("pages/host_health.py",     label="🖥️ Host Health")
    st.page_link("pages/log_search.py",      label="🔎 Log Search")
    st.page_link("pages/upload_logs.py",     label="📤 Upload & Analyze")
    st.divider()


# ---------------------------------------------------------------------------
# Page header
# ---------------------------------------------------------------------------
st.markdown(
    """
    <div style='background: linear-gradient(135deg, #0f172a 0%, #111827 45%, #1e293b 100%);
                padding: 1.4rem 2rem; border-radius: 14px; margin-bottom: 1.2rem;
                border: 1px solid rgba(148,163,184,0.18);'>
      <div style='display:flex; align-items:center; gap:10px;'>
        <span style='font-size:1.6rem;'>📤</span>
        <div>
          <div style='font-size:1.4rem; font-weight:800; color:#ffffff; letter-spacing:-0.02em;'>
            Upload & Analyze
          </div>
          <div style='color:#94a3b8; font-size:0.88rem; margin-top:2px;'>
            Drag in one or more <code>.log</code> or <code>.txt</code> files and run the
            full analysis pipeline — no CLI required.
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
        "Drop .log or .txt files here (multiple files allowed)",
        accept_multiple_files=True,
        type=["log", "txt"],
    )

    dry_run_toggle = st.checkbox(
        "Dry run (skip Postgres write)",
        value=False,
        help="Real run is the default; results are written to Postgres. "
             "Enable this to test parsing without touching the database.",
    )

    analyze_disabled = not uploaded_files
    analyze_clicked  = st.button(
        "🚀 Analyze",
        type="primary",
        disabled=analyze_disabled,
        use_container_width=False,
    )

    if analyze_disabled:
        st.caption("Upload at least one file to enable analysis.")

    if analyze_clicked and uploaded_files:
        # ── Stage files ──────────────────────────────────────────────────
        batch_id  = _make_batch_id()
        batch_dir = UPLOADS_ROOT / Path(batch_id).name
        try:
            staged = _stage_files(uploaded_files, batch_dir)
        except Exception as exc:
            st.error(f"Failed to stage uploaded files: {exc}")
            st.stop()

        # ── Launch pipeline ───────────────────────────────────────────────
        try:
            proc, log_path = _launch_pipeline(batch_dir, dry_run=dry_run_toggle)
        except Exception as exc:
            st.error(f"Failed to launch pipeline: {exc}")
            st.stop()

        # ── Persist state ─────────────────────────────────────────────────
        st.session_state.job_batch_id  = batch_id
        st.session_state.job_batch_dir = batch_dir
        st.session_state.job_log_path  = log_path
        st.session_state.job_proc      = proc
        st.session_state.job_status    = "running"
        st.session_state.job_dry_run   = dry_run_toggle
        st.session_state.job_staged    = [f.name for f in staged]

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
        st.info(
            f"**Batch:** `{st.session_state.job_batch_id}`  \n"
            f"**Files staged:** {', '.join(st.session_state.job_staged)}  \n"
            f"**Dry run:** {'Yes' if st.session_state.job_dry_run else 'No'}"
        )
    with meta_col:
        st.metric("Status", "⏳ Running")

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
            f"✅ Pipeline completed successfully — batch `{st.session_state.job_batch_id}`"
        )
    else:
        st.error(
            f"❌ Pipeline failed — batch `{st.session_state.job_batch_id}`"
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
            st.markdown("---")
            st.subheader("📊 Result Summary")

            # ── Overview metrics ─────────────────────────────────────────
            total_rows      = len(df)
            total_anomalies = int(df["is_anomaly"].sum()) if "is_anomaly" in df.columns else "N/A"
            label_col_exists = "label" in df.columns
            total_incidents = (
                df[df["label"].isin(["medium", "critical"])].shape[0]
                if label_col_exists else "N/A"
            )

            m1, m2, m3 = st.columns(3)
            m1.metric("Total rows processed", f"{total_rows:,}")
            m2.metric("Anomalies detected",   total_anomalies if isinstance(total_anomalies, str) else f"{total_anomalies:,}")
            m3.metric("Incidents (med+crit)", total_incidents if isinstance(total_incidents, str) else f"{total_incidents:,}")

            # ── Severity distribution ────────────────────────────────────
            if label_col_exists:
                st.markdown("#### Severity Distribution")
                counts = df["label"].value_counts().to_dict()

                sev_cols = st.columns(len(LABEL_ORDER))
                for col, label in zip(sev_cols, LABEL_ORDER):
                    count = counts.get(label, 0)
                    color = LABEL_COLORS.get(label, "#64748b")
                    col.markdown(
                        f"""
                        <div style='background:{color}18; border:1px solid {color}44;
                                    border-radius:10px; padding:0.7rem 1rem; text-align:center;'>
                          <div style='font-size:1.6rem; font-weight:800; color:{color};'>{count:,}</div>
                          <div style='font-size:0.78rem; color:#475569; text-transform:uppercase;
                                      letter-spacing:0.12em; margin-top:2px;'>{label}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

            # ── Top incidents ────────────────────────────────────────────
            if "combined_score" in df.columns:
                st.markdown("#### Top 10 Incidents by Combined Score")

                display_cols = [
                    c for c in
                    ["timestamp", "host", "label", "combined_score", "correlation_id"]
                    if c in df.columns
                ]

                top_df = (
                    df.sort_values("combined_score", ascending=False)
                    .head(10)[display_cols]
                    .reset_index(drop=True)
                )

                # Colour-code the label column
                def _style_label(val: str) -> str:
                    color = LABEL_COLORS.get(str(val).lower(), "#475569")
                    return f"color: {color}; font-weight: 600;"

                styled = top_df.style
                if "label" in top_df.columns:
                    styled = styled.applymap(_style_label, subset=["label"])
                if "combined_score" in top_df.columns:
                    styled = styled.format({"combined_score": "{:.4f}"})

                st.dataframe(styled, use_container_width=True)

    # ── Restart button ───────────────────────────────────────────────────────
    st.markdown("---")
    if st.button("🔄 Upload more files / start new analysis", type="secondary"):
        # Clear job state so the upload form reappears
        for key in [
            "job_batch_id", "job_batch_dir", "job_log_path",
            "job_proc", "job_status", "job_dry_run", "job_staged",
        ]:
            st.session_state[key] = None if key != "job_staged" else []
        st.rerun()