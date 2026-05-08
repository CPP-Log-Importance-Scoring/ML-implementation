import pandas as pd
import numpy as np

def add_counter_proximity(df):

    anomaly_templates = [
        "IF_DOWN",
        "PORT_SCAN"
    ]

    anomaly_times = df[
        df["template_id"].isin(anomaly_templates)
    ][["session_id", "timestamp"]]

    scores = []

    for _, row in df.iterrows():

        session = row["session_id"]
        ts = row["timestamp"]

        subset = anomaly_times[
            anomaly_times["session_id"] == session
        ]

        if subset.empty:
            scores.append(0.0)
            continue

        min_diff = (
            subset["timestamp"] - ts
        ).abs().dt.total_seconds().min()

        if min_diff <= 30:
            score = np.exp(-min_diff / 30)
        else:
            score = 0.0

        scores.append(score)

    df["counter_proximity"] = scores

    return df