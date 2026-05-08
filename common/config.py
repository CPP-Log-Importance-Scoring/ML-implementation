"""
common/config.py
================
Central configuration for all feature thresholds and constants.
All magic numbers live here — never hardcode in module files.
"""


# Severity weights

SEVERITY_WEIGHTS: dict[str, float] = {
    "CRITICAL": 1.0,
    "ERROR":    0.7,
    "WARN":     0.4,
    "INFO":     0.1,
}

# Fallback weight for unknown severity levels
DEFAULT_SEVERITY_WEIGHT: float = 0.1


# Counter-anomaly proximity
# Logs occurring near interface counter anomalies receive
# higher anomaly proximity scores.

# ± time window around anomaly event
COUNTER_PROXIMITY_WINDOW_SECONDS: int = 30

# Exponential decay coefficient for proximity scoring
COUNTER_PROXIMITY_DECAY_RATE: float = 0.05


# Statistical feature configuration

# Rolling baseline window size for z-score computation
ZSCORE_ROLLING_WINDOW: int = 20

# Small epsilon to avoid divide-by-zero
ZSCORE_MIN_STD: float = 1e-6

# Minimum number of events needed to compute burstiness
BURSTINESS_MIN_EVENTS: int = 2


# Temporal feature configuration

# Rolling window size for inter-arrival rate calculation
INTER_ARRIVAL_ROLLING_WINDOW: int = 5


# Input / Output paths

# Sessionized logs produced by parsing layer
SESSIONIZED_LOGS_PATH: str = (
    "data/processed/sessionized_logs.parquet"
)

# Final feature dataframe output
FEATURES_OUTPUT_PATH: str = (
    "data/processed/features_df.parquet"
)


# Feature dataframe schema contract
# Shared contract between:
# P1 (Features) -> P2 (ML) -> P4 (Scoring)

FEATURE_COLUMNS: list[str] = [
    "log_id",
    "session_id",
    "frequency_score",
    "burstiness_score",
    "zscore_base",
    "time_delta_prev",
    "severity_weight",
    "counter_proximity",
]