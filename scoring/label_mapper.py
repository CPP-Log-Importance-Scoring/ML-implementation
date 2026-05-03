# scoring/label_mapper.py

"""
Maps final_score → label
"""

from common.config import LABEL_THRESHOLDS


def map_score_to_label(score):
    for label, (low, high) in LABEL_THRESHOLDS.items():
        if low <= score < high:
            return label
    return "critical"