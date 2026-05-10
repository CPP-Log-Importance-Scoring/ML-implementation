"""
label_mapper.py
===============

Maps final_score to human-readable severity labels.

Labels:
    ignore
    low
    medium
    critical
"""

from pathlib import Path

import pandas as pd

from common.config import LABEL_THRESHOLDS
from common.logger import get_logger

logger = get_logger(__name__)

DATA_DIR = Path("data/processed")

SCORED_PATH = DATA_DIR / "scored_logs_df.parquet"


def map_label(score: float) -> str:

    for label, (lower, upper) in LABEL_THRESHOLDS.items():

        if lower <= score < upper:
            return label

    # Edge case: exactly 1.0
    return "critical"


def run():

    logger.info("Loading scored logs...")

    df = pd.read_parquet(SCORED_PATH)

    logger.info("Mapping labels from final_score...")

    df["label"] = df["final_score"].apply(map_label)

    df.to_parquet(
        SCORED_PATH,
        index=False,
    )

    logger.info(
        "Labels added successfully."
    )

    logger.info(
        f"Label distribution:\n"
        f"{df['label'].value_counts()}"
    )

    return df


if __name__ == "__main__":

    run()