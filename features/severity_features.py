SEVERITY_MAP = {
    "CRITICAL": 1.0,
    "ERROR": 0.7,
    "WARN": 0.4,
    "INFO": 0.1
}

def add_severity_weight(df):

    df["severity_weight"] = (
        df["log_level"]
        .map(SEVERITY_MAP)
        .fillna(0.1)
    )

    return df