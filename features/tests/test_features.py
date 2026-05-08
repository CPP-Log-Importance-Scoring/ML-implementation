import pandas as pd
from datetime import datetime

from features.severity_features import add_severity_weight

def test_severity_weight():

    df = pd.DataFrame({
        "log_level": ["CRITICAL", "INFO"]
    })

    result = add_severity_weight(df)

    assert result["severity_weight"].iloc[0] == 1.0
    assert result["severity_weight"].iloc[1] == 0.1