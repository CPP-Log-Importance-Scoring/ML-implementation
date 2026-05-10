"""
incident_clusterer.py
=====================

Groups related logs into incidents using DBSCAN.
"""

from pathlib import Path

import pandas as pd

from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

from common.config import (
    DBSCAN_EPS,
    DBSCAN_MIN_SAMPLES,
)

from common.logger import get_logger

logger = get_logger(__name__)

DATA_DIR = Path("data/processed")

SCORED_PATH = DATA_DIR / "scored_logs_df.parquet"


def run():

    logger.info("Loading scored logs...")

    df = pd.read_parquet(SCORED_PATH)

    # ---------------------------------------------------------------
    # Feature vector for clustering
    # ---------------------------------------------------------------

    cluster_features = [
        "final_score",
        "centrality_score",
        "time_delta_session_start",
    ]

    logger.info(
        f"Using clustering features: {cluster_features}"
    )

    X = df[cluster_features].fillna(0.0)

    # ---------------------------------------------------------------
    # Normalization
    # ---------------------------------------------------------------

    scaler = StandardScaler()

    X_scaled = scaler.fit_transform(X)

    # ---------------------------------------------------------------
    # DBSCAN clustering
    # ---------------------------------------------------------------

    dbscan = DBSCAN(
        eps=DBSCAN_EPS,
        min_samples=DBSCAN_MIN_SAMPLES,
    )

    clusters = dbscan.fit_predict(X_scaled)

    df["cluster"] = clusters

    # ---------------------------------------------------------------
    # Convert cluster IDs to incident IDs
    # ---------------------------------------------------------------

    incident_ids = []

    for cluster_id in clusters:

        if cluster_id == -1:

            incident_ids.append(None)

        else:

            incident_ids.append(
                f"INC-{cluster_id:03d}"
            )

    df["incident_id"] = incident_ids

    # ---------------------------------------------------------------
    # Cluster summary
    # ---------------------------------------------------------------

    n_incidents = len(
        set(clusters) - {-1}
    )

    noise_ratio = (
        (clusters == -1).sum() / len(clusters)
    )

    avg_cluster_size = (
        df[df["cluster"] != -1]
        .groupby("cluster")
        .size()
        .mean()
    )

    logger.info(
        f"Incidents found: {n_incidents}"
    )

    logger.info(
        f"Noise ratio: {noise_ratio:.2%}"
    )

    logger.info(
        f"Average cluster size: {avg_cluster_size}"
    )

    # ---------------------------------------------------------------
    # Save updated dataframe
    # ---------------------------------------------------------------

    df.to_parquet(
        SCORED_PATH,
        index=False,
    )

    logger.info(
        "Incident clustering completed."
    )

    return df


if __name__ == "__main__":

    run()