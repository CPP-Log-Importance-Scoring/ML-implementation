from common.env_handler import get_env

# ---------------------------------------------------------------------------
# Correlation / Graph parameters
# ---------------------------------------------------------------------------

# Time window (seconds) within which two log events are considered co-occurring.
# Widening this produces a denser graph; narrowing it produces a sparser one.
CORRELATION_TIME_WINDOW_SECONDS: int = 60

# Hard cap on the number of template nodes admitted into the correlation graph.
# Only the MAX_GRAPH_NODES most-frequent templates are kept; the rest are
# silently dropped before edge construction begins.  Raising this limit
# increases memory and CPU cost roughly as O(N^2) in the worst case because
# the sliding-window join visits every pair of events that fall inside the
# same time window.  500 is a safe default for a single-host deployment;
# reduce to ~100 for memory-constrained environments or increase carefully
# after profiling.
MAX_GRAPH_NODES: int = 500


# ---------------------------------------------------------------------------
# Dynamic environment-variable access (credentials, service URLs, etc.)
# ---------------------------------------------------------------------------

def __getattr__(name: str):
    """
    Dynamically fetch environment variables when they are accessed.
    This allows lazy evaluation: variables are only checked when actually imported/used.
    """
    if name.startswith("__"):
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    # Fetch the requested variable (e.g., DB_URL, ELASTIC_URL) directly from the .env file
    return get_env(name)

# ----------------------------
# SCORING WEIGHTS
# Controls contribution of each signal to final score
# ----------------------------
ML_WEIGHT: float = 0.4
GRAPH_WEIGHT: float = 0.35
RULE_WEIGHT: float = 0.25

# ----------------------------
# LABEL THRESHOLDS
# ----------------------------
LABEL_THRESHOLDS = {
    "ignore": (0.0, 0.2),
    "low": (0.2, 0.5),
    "medium": (0.5, 0.75),
    "critical": (0.75, 1.0),
}

# ----------------------------
# DBSCAN PARAMETERS
# ----------------------------
DBSCAN_EPS: float = 0.5
DBSCAN_MIN_SAMPLES: int = 5