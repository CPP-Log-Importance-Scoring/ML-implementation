import pandas as pd
import numpy as np

def log_frequency_score(df):

    freq = (
        df.groupby(["session_id", "template_id"])
        ["template_id"]
        .transform("count")
    )

    df["frequency_score"] = freq / freq.max()

    return df


def burstiness_score(df):

    df = df.sort_values(
        ["session_id", "timestamp"]
    )

    df["time_diff"] = (
        df.groupby("session_id")["timestamp"]
        .diff()
        .dt.total_seconds()
    )

    variance = (
        df.groupby("session_id")["time_diff"]
        .transform(lambda x: x.var())
    )

    max_var = variance.max()

    if max_var == 0 or pd.isna(max_var):
        df["burstiness_score"] = 0.0
    else:
        df["burstiness_score"] = variance / max_var

    return df


def zscore_base(df):

    counts = (
        df.groupby("template_id")
        .size()
    )

    mean = counts.mean()
    std = counts.std()

    zmap = (
        (counts - mean) / std
        if std != 0 else counts * 0
    )

    df["zscore_base"] = df["template_id"].map(zmap)

    return df