"""
scoring/incident_clusterer.py

Group related logs into incidents via ANOMALY-SEEDED TEMPORAL WINDOWING.

Why not DBSCAN on scores?
    The previous approach ran DBSCAN over [final_score, centrality_score,
    temporal_proximity]. That clusters by score-similarity, not time/causality,
    so (a) the dense benign mass collapsed into incidents spanning up to ~23h
    and (b) the rare high-score anomalies fell below DBSCAN's density floor and
    were discarded as noise — i.e. the rows you most want IN an incident were
    the ones thrown out (verified: 0/21 critical rows clustered).

This module instead:
    1. SEEDS on the interesting rows only — is_anomaly OR final_score high OR
       severity (event_weight) high.
    2. Groups seeds that fall within INCIDENT_WINDOW_SECONDS of each other in
       ABSOLUTE time. Gaps are measured between seeds (sparse), so continuous
       background noise cannot bridge two incidents and an incident stays a
       bounded, minutes-long burst.
    3. Drops groups with fewer than INCIDENT_MIN_SEEDS seeds, so isolated
       false-positive seeds on a clean day form no incident.

cluster_id (the "C0000" graph-community string from P3) is left untouched.

Public API
----------
cluster_incidents(scored_df) -> pd.DataFrame
    Adds correlation_id and is_cross_system columns; returns updated df.

run() -> pd.DataFrame
    Thin wrapper: loads scored_logs_df.parquet and calls cluster_incidents().
"""

from __future__ import annotations

import numpy as np
import pandas as pd

import common.config as cfg
from common.logger import get_logger
from common.utils import load_parquet

logger = get_logger(__name__)

_SCORED_PATH = "data/processed/scored_logs_df.parquet"


def _seed_mask(df: pd.DataFrame) -> pd.Series:
    """Boolean mask of incident-seed rows: anomalous, high-score, or high-severity.

    Never seeds on an 'ignore'-labelled row (pure benign noise), so a stray
    is_anomaly flag on an otherwise-benign line can't anchor an incident.
    """
    seed = pd.Series(False, index=df.index)
    if "is_anomaly" in df.columns:
        seed |= df["is_anomaly"].fillna(False).astype(bool)
    if "label" in df.columns:
        seed |= df["label"].isin(cfg.INCIDENT_SEED_LABELS)
    if "final_score" in df.columns:
        seed |= df["final_score"] >= cfg.INCIDENT_SEED_SCORE_MIN
    if "event_weight" in df.columns:
        seed |= df["event_weight"] >= cfg.INCIDENT_SEED_SEVERITY_MIN
    if "label" in df.columns:
        seed &= df["label"] != "ignore"
    return seed


def cluster_incidents(scored_df: pd.DataFrame) -> pd.DataFrame:
    """Group seed rows into incidents by absolute-time windowing.

    Parameters
    ----------
    scored_df : pd.DataFrame
        Must contain: timestamp, final_score, label, cluster_id, and ideally
        is_anomaly / event_weight (used for seeding).

    Returns
    -------
    pd.DataFrame
        Input df with correlation_id and is_cross_system columns added.
        cluster_id is unchanged (still "C0000"-format strings from P3).
    """
    df = scored_df.copy()
    df["correlation_id"] = None
    df["is_cross_system"] = False

    if "timestamp" not in df.columns:
        logger.warning("No timestamp column — cannot form temporal incidents.")
        return df

    seed = _seed_mask(df)
    n_seeds = int(seed.sum())
    if n_seeds == 0:
        logger.info("No seed rows — no incidents formed (clean batch).")
        return df

    # Group seeds by absolute-time gap.
    work = df.loc[seed, ["timestamp"]].copy()
    work["timestamp"] = pd.to_datetime(work["timestamp"], errors="coerce")
    work = work.dropna(subset=["timestamp"]).sort_values("timestamp")

    gap = work["timestamp"].diff().dt.total_seconds()
    grp = (gap > float(cfg.INCIDENT_WINDOW_SECONDS)).cumsum()

    # Density floor: drop groups with too few seeds (isolated false positives).
    sizes = grp.map(grp.value_counts())
    grp = grp.where(sizes >= cfg.INCIDENT_MIN_SEEDS, other=np.nan)

    # Renumber surviving groups chronologically → INC-NNNN.
    valid = grp.dropna()
    if len(valid):
        firsts = (
            work.loc[valid.index]
            .assign(_g=valid)
            .groupby("_g")["timestamp"].min()
            .sort_values()
        )
        id_map = {g: f"INC-{i:04d}" for i, g in enumerate(firsts.index)}
        ids = grp.map(lambda g: id_map.get(g) if pd.notna(g) else None)
    else:
        ids = grp  # all NaN

    df.loc[work.index, "correlation_id"] = ids.values

    valid_incidents = df["correlation_id"].dropna().unique()
    sizes_out = df[df["correlation_id"].notna()].groupby("correlation_id").size()
    if len(sizes_out):
        spans = (
            df[df["correlation_id"].notna()]
            .assign(_ts=pd.to_datetime(df.loc[df["correlation_id"].notna(), "timestamp"]))
            .groupby("correlation_id")["_ts"]
            .agg(lambda s: (s.max() - s.min()).total_seconds())
        )
        max_span_min = float(spans.max()) / 60.0
    else:
        max_span_min = 0.0
    logger.info(
        "Incidents: %d from %d seeds (largest=%d rows, max_span=%.1f min).",
        len(valid_incidents), n_seeds,
        int(sizes_out.max()) if len(sizes_out) else 0, max_span_min,
    )

    # is_cross_system: incident touches >1 graph community.
    n_cross_system = 0
    for cid in valid_incidents:
        mask = df["correlation_id"] == cid
        if "cluster_id" in df.columns and df.loc[mask, "cluster_id"].nunique() > 1:
            df.loc[mask, "is_cross_system"] = True
            n_cross_system += 1
    logger.info("Cross-system incidents: %d", n_cross_system)

    return df


def run() -> pd.DataFrame:
    """Thin wrapper: load scored_logs_df.parquet and call cluster_incidents()."""
    df = load_parquet(_SCORED_PATH)
    return cluster_incidents(df)
