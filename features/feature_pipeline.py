import pandas as pd

from statistical_features import (
    log_frequency_score,
    burstiness_score,
    zscore_base
)

from temporal_features import add_temporal_features

from severity_features import add_severity_weight

from counter_proximity import add_counter_proximity


def build_features(input_path):

    df = pd.read_parquet(input_path)

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df = log_frequency_score(df)

    df = burstiness_score(df)

    df = zscore_base(df)

    df = add_temporal_features(df)

    df = add_severity_weight(df)

    df = add_counter_proximity(df)

    features_df = df[[
        "log_id",
        "session_id",
        "frequency_score",
        "burstiness_score",
        "zscore_base",
        "time_delta_prev",
        "severity_weight",
        "counter_proximity"
    ]]

    return features_df


if __name__ == "__main__":

    features = build_features(
    "parsing/processed/sessionized_logs.parquet"
)

    print(features.head())


features.to_parquet(
    "data/processed/features_df.parquet",
    index=False
)

print("features saved")