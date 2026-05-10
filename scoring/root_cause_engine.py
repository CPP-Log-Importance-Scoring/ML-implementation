"""
root_cause_engine.py
====================

Identifies likely root-cause logs within each incident cluster.
"""

from pathlib import Path

import pandas as pd

from common.logger import get_logger

logger = get_logger(__name__)

DATA_DIR = Path("data/processed")

SCORED_PATH = DATA_DIR / "scored_logs_df.parquet"

ROOT_CAUSE_OUTPUT = DATA_DIR / "root_causes_df.parquet"

OUTPUT_COLUMNS = [
    "log_id",
    "final_score",
    "label",
    "incident_id",
    "is_root_cause",
    "root_cause_confidence",
]

def run():

    logger.info("Loading clustered scored logs...")

    df = pd.read_parquet(SCORED_PATH)

    # ---------------------------------------------------------------
    # Default values
    # ---------------------------------------------------------------

    df["is_root_cause"] = False

    df["root_cause_confidence"] = 0.0

    root_cause_rows = []

    # ---------------------------------------------------------------
    # Process each incident
    # ---------------------------------------------------------------

    valid_incidents = (
        df["incident_id"]
        .dropna()
        .unique()
    )

    logger.info(
        f"Processing {len(valid_incidents)} incidents..."
    )

    for incident_id in valid_incidents:

        cluster_df = df[
            df["incident_id"] == incident_id
        ].copy()

        # -----------------------------------------------------------
        # Rank by centrality_score descending
        # -----------------------------------------------------------

        cluster_df = cluster_df.sort_values(
            by="centrality_score",
            ascending=False,
        )

        max_centrality = (
            cluster_df["centrality_score"].max()
        )

        # Prevent divide-by-zero
        if max_centrality == 0:
            max_centrality = 1.0

        # -----------------------------------------------------------
        # Top 3 logs become root-cause candidates
        # -----------------------------------------------------------

        top_candidates = cluster_df.head(3)

        for _, row in top_candidates.iterrows():

            confidence = (
                row["centrality_score"]
                / max_centrality
            )

            # Update scored dataframe
            df.loc[
                df["log_id"] == row["log_id"],
                "is_root_cause"
            ] = True

            df.loc[
                df["log_id"] == row["log_id"],
                "root_cause_confidence"
            ] = confidence

            root_cause_rows.append({
                "incident_id": incident_id,
                "root_cause_log_id": row["log_id"],
                "confidence_score": confidence,
            })

    # ---------------------------------------------------------------
    # Save scored logs
    # ---------------------------------------------------------------

    final_output_df = df[OUTPUT_COLUMNS]

    final_output_df.to_parquet(
        SCORED_PATH,
        index=False,
    )
    # ---------------------------------------------------------------
    # Save root cause summary
    # ---------------------------------------------------------------

    root_causes_df = pd.DataFrame(
        root_cause_rows
    )

    root_causes_df.to_parquet(
        ROOT_CAUSE_OUTPUT,
        index=False,
    )

    logger.info(
        f"Root causes identified:"
        f" {len(root_causes_df)}"
    )

    logger.info(
        f"Saved root causes to:"
        f" {ROOT_CAUSE_OUTPUT}"
    )

    return df, root_causes_df


if __name__ == "__main__":

    run()