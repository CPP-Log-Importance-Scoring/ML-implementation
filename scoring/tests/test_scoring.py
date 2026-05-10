import pandas as pd


def test_final_score_range():

    df = pd.read_parquet(
        "data/processed/scored_logs_df.parquet"
    )

    assert (
        (df["final_score"] >= 0).all()
        and
        (df["final_score"] <= 1).all()
    )


def test_valid_labels():

    df = pd.read_parquet(
        "data/processed/scored_logs_df.parquet"
    )

    valid_labels = {
        "ignore",
        "low",
        "medium",
        "critical",
    }

    assert (
        df["label"]
        .isin(valid_labels)
        .all()
    )


def test_incident_id_format():

    df = pd.read_parquet(
        "data/processed/scored_logs_df.parquet"
    )

    valid = df["incident_id"].dropna()

    assert valid.str.startswith("INC-").all()


def test_root_cause_confidence_range():

    df = pd.read_parquet(
        "data/processed/scored_logs_df.parquet"
    )

    assert (
        (df["root_cause_confidence"] >= 0).all()
        and
        (df["root_cause_confidence"] <= 1).all()
    )


def test_root_causes_schema():

    df = pd.read_parquet(
        "data/processed/root_causes_df.parquet"
    )

    required_cols = {
        "incident_id",
        "root_cause_log_id",
        "confidence_score",
    }

    assert required_cols.issubset(df.columns)