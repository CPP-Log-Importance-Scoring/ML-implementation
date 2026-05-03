# scoring/importance_scorer.py

"""
Importance Scorer Module

Combines:
- ML anomaly scores (P2)
- Graph correlation scores (P3)
- Rule-based scores (P1)

Output:
- Final importance score per log
"""

import pandas as pd
from common.config import ML_WEIGHT, GRAPH_WEIGHT, RULE_WEIGHT


def compute_importance_score(anomaly_df, graph_scores_df, features_df):
    """
    Parameters:
        anomaly_df: DataFrame with ML anomaly scores
        graph_scores_df: DataFrame with graph-based scores
        features_df: DataFrame with rule-based features

    Returns:
        scored_logs_df: DataFrame with final scores
    """

    # TODO: merge all inputs on log_id
    # TODO: compute weighted score

    pass