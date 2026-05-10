"""
importance_scorer.py
====================

Phase 2 — Full Importance Score Integration
Assignee: Ujwal Hegde

Loads:
    - features_df.parquet
    - anomaly_df.parquet
    - graph_scores_df.parquet

Merges all signals into a final importance score.

Output:
    scored_logs_df.parquet
"""

from pathlib import Path

import pandas as pd

from common.config import (
    ML_WEIGHT,
    GRAPH_WEIGHT,
    RULE_WEIGHT,
)

from common.logger import get_logger

logger = get_logger(__name__)

# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

DATA_DIR = Path("data/processed")

FEATURES_PATH = DATA_DIR / "features_df.parquet"
ANOMALY_PATH = DATA_DIR / "anomaly_df.parquet"
GRAPH_PATH = DATA_DIR / "graph_scores_df.parquet"

OUTPUT_PATH = DATA_DIR / "scored_logs_df.parquet"


# -------------------------------------------------------------------
# Load helpers
# -------------------------------------------------------------------

def load_inputs():

    logger.info("Loading parquet inputs...")

    # ---------------------------------------------------------------
    # Features (mandatory)
    # ---------------------------------------------------------------

    if not FEATURES_PATH.exists():

        raise FileNotFoundError(
            "features_df.parquet not found. "
            "Feature pipeline must run before scoring."
        )

    features_df = pd.read_parquet(FEATURES_PATH)

    # ---------------------------------------------------------------
    # Anomaly scores (graceful fallback)
    # ---------------------------------------------------------------

    if ANOMALY_PATH.exists():

        anomaly_df = pd.read_parquet(ANOMALY_PATH)

        logger.info(
            f"Loaded anomaly_df "
            f"({len(anomaly_df)} rows)"
        )

    else:

        logger.warning(
            "anomaly_df.parquet not found. "
            "Using fallback combined_score = 0.0"
        )

        anomaly_df = pd.DataFrame({
            "log_id": features_df["log_id"],
            "combined_score": 0.0,
        })

    # ---------------------------------------------------------------
    # Graph scores (graceful fallback)
    # ---------------------------------------------------------------

    if GRAPH_PATH.exists():

        graph_scores_df = pd.read_parquet(GRAPH_PATH)

        logger.info(
            f"Loaded graph_scores_df "
            f"({len(graph_scores_df)} rows)"
        )

    else:

        logger.warning(
            "graph_scores_df.parquet not found. "
            "Using fallback centrality_score = 0.0"
        )

        graph_scores_df = pd.DataFrame({
            "log_id": features_df["log_id"],
            "centrality_score": 0.0,
        })

    logger.info(
        f"Loaded:"
        f" features={len(features_df)},"
        f" anomaly={len(anomaly_df)},"
        f" graph={len(graph_scores_df)}"
    )

    return features_df, anomaly_df, graph_scores_df


# -------------------------------------------------------------------
# Merge all signals
# -------------------------------------------------------------------

def merge_inputs(
    features_df: pd.DataFrame,
    anomaly_df: pd.DataFrame,
    graph_scores_df: pd.DataFrame,
):

    logger.info("Merging inputs on log_id...")

    merged_df = features_df.merge(
        anomaly_df,
        on="log_id",
        how="outer",
    )

    merged_df = merged_df.merge(
        graph_scores_df,
        on="log_id",
        how="outer",
    )

    # ---------------------------------------------------------------
    # Fill missing scores
    # ---------------------------------------------------------------

    score_cols = [
        "combined_score",
        "centrality_score",
        "severity_weight",
    ]

    for col in score_cols:

        missing = merged_df[col].isnull().sum()

        if missing > 0:
            logger.warning(
                f"{missing} missing values in {col} "
                f"filled with 0.0"
            )

            merged_df[col] = merged_df[col].fillna(0.0)

    return merged_df


# -------------------------------------------------------------------
# Final score computation
# -------------------------------------------------------------------

def compute_final_score(df: pd.DataFrame):

    logger.info("Computing final importance score...")

    df["final_score"] = (
        (ML_WEIGHT * df["combined_score"])
        + (GRAPH_WEIGHT * df["centrality_score"])
        + (RULE_WEIGHT * df["severity_weight"])
    )

    # Keep scores within [0,1]
    df["final_score"] = df["final_score"].clip(0.0, 1.0)

    logger.info(
        f"Final score range:"
        f" [{df['final_score'].min():.4f},"
        f" {df['final_score'].max():.4f}]"
    )

    return df


# -------------------------------------------------------------------
# Save output
# -------------------------------------------------------------------

def save_output(df: pd.DataFrame):

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        OUTPUT_PATH,
        index=False,
    )

    logger.info(
        f"Saved scored logs to {OUTPUT_PATH}"
    )


# -------------------------------------------------------------------
# Main pipeline
# -------------------------------------------------------------------

def run():

    features_df, anomaly_df, graph_scores_df = load_inputs()

    merged_df = merge_inputs(
        features_df,
        anomaly_df,
        graph_scores_df,
    )

    scored_logs_df = compute_final_score(merged_df)

    save_output(scored_logs_df)

    return scored_logs_df


if __name__ == "__main__":

    run()