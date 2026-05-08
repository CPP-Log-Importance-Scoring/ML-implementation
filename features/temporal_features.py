import pandas as pd

def add_temporal_features(df):

    df = df.sort_values(["session_id", "timestamp"])

    df["time_delta_prev"] = (
        df.groupby("session_id")["timestamp"]
        .diff()
        .dt.total_seconds()
        .fillna(0)
    )

    session_start = (
        df.groupby("session_id")["timestamp"]
        .transform("min")
    )

    df["time_delta_session_start"] = (
        df["timestamp"] - session_start
    ).dt.total_seconds()

    rolling_mean = (
        df.groupby("session_id")["time_delta_prev"]
        .transform(
            lambda x: x.rolling(5, min_periods=1).mean()
        )
    )

    df["inter_arrival_rate"] = 1 / (rolling_mean + 1e-5)

    return df