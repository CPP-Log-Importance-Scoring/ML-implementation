"""
scoring/drift_scorer.py

Deterministic per-component event-rate drift signal.

Why this exists
---------------
The IsolationForest features are point-in-time / burstiness based. A gradual
failure that emits one log every ~40s (an OOM-kill cascade spread over 17
minutes, a slow memory leak, thermal creep) has no burst signature, so the
model misses it (verified 2026-06-22 on the anomaly_days fixtures). This module
recovers that signal without the model and without per-dataset tuning.

Method
------
Bin the run into fixed `DRIFT_BIN_SECONDS` windows. For each component, count
events per bin across the FULL run range (silent bins counted as zero), then
z-score every bin against that component's own mean/std. A component that is
normally quiet but suddenly active — as a tight burst or a slow drip across
many bins — produces high-z bins. Each event inherits its bin's z-score,
mapped to drift_score ∈ [0, 1] in the elevated direction only.

Self-calibrating per component → robust to cold-start and to a contaminated
training set. Components with too little history (< DRIFT_MIN_COMPONENT_EVENTS)
or zero variance contribute nothing.

STATUS — disabled by default (SCORING_DRIFT_ENABLED=False)
---------------------------------------------------------
Validated 2026-06-22 on clean_days + anomaly_days and found NOT separable with a
within-file baseline: the clean day reaches a per-component rate-ratio of 3.0,
while the real PROTOCOL_STARVATION component (already high-rate) reaches 1.14 and
the OOM MEMORY_MANAGER (active all day via the leak) reaches 3.0. The anomaly's
own component spikes no more than normal periodic chatter, so no single
threshold catches the anomalies without flooding the clean day. The fix is a
CROSS-DAY baseline (compare a component's rate to its rate on known-clean days),
which needs the clean-baseline corpus. This module is the foundation for that
version; the within-file math here is intentionally left off until then.

Public API
----------
compute_drift_scores(sessionized_df) -> pd.DataFrame[sequence_number, drift_score]
"""

from __future__ import annotations

import numpy as np
import pandas as pd

import common.config as cfg
from common.logger import get_logger

logger = get_logger(__name__)


def compute_drift_scores(sessionized_df: pd.DataFrame) -> pd.DataFrame:
    """Return a per-log drift_score ∈ [0, 1] from per-component rate elevation.

    Parameters
    ----------
    sessionized_df : pd.DataFrame
        Must contain: sequence_number, timestamp, component.

    Returns
    -------
    pd.DataFrame with columns [sequence_number, drift_score]. Zero for every
    row when the inputs are degenerate (single bin, no qualifying components),
    so callers can merge unconditionally.
    """
    required = {"sequence_number", "timestamp", "component"}
    missing = required - set(sessionized_df.columns)
    if missing:
        logger.warning("compute_drift_scores: missing columns %s — returning zeros", missing)
        return pd.DataFrame({
            "sequence_number": sessionized_df.get("sequence_number", pd.Series(dtype=int)),
            "drift_score": 0.0,
        })

    df = sessionized_df[["sequence_number", "timestamp", "component"]].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["component"] = df["component"].fillna("UNKNOWN").astype(str)

    t0 = df["timestamp"].min()
    span = (df["timestamp"].max() - t0).total_seconds()
    drift = pd.Series(0.0, index=df.index)

    # Degenerate run (everything in one bin) → no rate signal to extract.
    if not np.isfinite(span) or span < cfg.DRIFT_BIN_SECONDS:
        logger.info("compute_drift_scores: run span < one bin — drift all zero.")
        return pd.DataFrame({
            "sequence_number": df["sequence_number"].values,
            "drift_score": drift.values,
        })

    df["bin"] = ((df["timestamp"] - t0).dt.total_seconds() // cfg.DRIFT_BIN_SECONDS).astype(int)
    n_bins = int(df["bin"].max()) + 1
    counts = df.groupby(["component", "bin"]).size().rename("cnt").reset_index()

    denom = max(cfg.DRIFT_RATIO_FULL - cfg.DRIFT_RATIO_MIN, 1e-9)
    n_components_used = 0
    for comp, g in counts.groupby("component"):
        active_counts = g["cnt"].to_numpy()
        if int(active_counts.sum()) < cfg.DRIFT_MIN_COMPONENT_EVENTS:
            continue
        if active_counts.size < cfg.DRIFT_MIN_ACTIVE_BINS:
            # Too few active bins to establish a "typical" rate to beat — a
            # single concentrated cluster would otherwise be its own baseline.
            continue
        # Typical ACTIVE rate (median over non-zero bins). A bin must exceed
        # THIS to count as elevated, so a steady periodic component (every
        # active bin ≈ median) never registers; only a genuine rate spike or a
        # sustained multi-bin elevation above the component's own norm does.
        med = float(np.median(active_counts))
        if med <= 0:
            continue
        full = np.zeros(n_bins, dtype=float)
        full[g["bin"].to_numpy()] = active_counts
        ratio = full / med
        drift_bin = np.clip((ratio - cfg.DRIFT_RATIO_MIN) / denom, 0.0, 1.0)
        if not drift_bin.any():
            continue

        mask = (df["component"] == comp).to_numpy()
        drift.iloc[mask] = drift_bin[df.loc[mask, "bin"].to_numpy()]
        n_components_used += 1

    logger.info(
        "Drift signal: %d/%d components scored, %d/%d rows with drift>0 "
        "(max=%.3f, mean=%.4f).",
        n_components_used, counts["component"].nunique(),
        int((drift > 0).sum()), len(df), float(drift.max()), float(drift.mean()),
    )
    return pd.DataFrame({
        "sequence_number": df["sequence_number"].values,
        "drift_score": drift.values,
    })
