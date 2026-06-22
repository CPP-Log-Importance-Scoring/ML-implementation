from common.env_handler import get_env

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

# Console log level for all modules using common/logger.py.
# The logger reads this via os.environ["LOG_LEVEL"] to avoid a circular import.
# Set LOG_LEVEL=DEBUG in your .env for verbose output.
LOG_LEVEL: str = "INFO"

# ---------------------------------------------------------------------------
# Correlation / Graph parameters
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Graph construction — canonical names
# ---------------------------------------------------------------------------

# Time window (seconds) within which two log events are considered co-occurring.
# Widening this produces a denser graph; narrowing it produces a sparser one.
GRAPH_COOCCURRENCE_WINDOW_SECONDS: int = 60

# Hard cap on unique templates admitted into the co-occurrence graph.
# Only the top-N most-frequent templates are kept; the rest are excluded
# before edge construction.  500 is a safe default for a single-host
# deployment; reduce to ~100 for memory-constrained environments.
GRAPH_MAX_NODES: int = 500

# PageRank damping factor (standard value; literature range 0.8–0.9).
GRAPH_PAGERANK_ALPHA: float = 0.85

# Backward-compatible aliases — kept so existing imports don't break.
CORRELATION_TIME_WINDOW_SECONDS: int = GRAPH_COOCCURRENCE_WINDOW_SECONDS
MAX_GRAPH_NODES: int = GRAPH_MAX_NODES
PAGERANK_ALPHA: float = GRAPH_PAGERANK_ALPHA

# Betweenness centrality approximation: number of pivot nodes sampled.
# Full exact computation is O(V*E) which is impractical for graphs > 200 nodes.
# k=50 gives a good bias-variance tradeoff for typical network-log graphs.
BETWEENNESS_K: int = 50
BETWEENNESS_LARGE_GRAPH_THRESHOLD: int = 200

# Sequence engine parameters.
# Window within which template B is considered a "follow-on" of template A.
SEQUENCE_WINDOW_SECONDS: int = 30
# A sequence must contain at least this many log templates.
SEQUENCE_MIN_LENGTH: int = 3
# A sequence must appear in at least this many distinct sessions.
SEQUENCE_MIN_SUPPORT: int = 5

# ---------------------------------------------------------------------------
# Phase 3 output paths
# ---------------------------------------------------------------------------
GRAPH_PICKLE_PATH: str = "data/processed/correlation_graph.gpickle"
GRAPH_JSON_PATH: str = "data/processed/correlation_graph.json"
SEQUENCES_JSON_PATH: str = "data/processed/sequences.json"
GRAPH_SCORES_PATH: str = "data/processed/graph_scores_df.parquet"
ANOMALY_PATH: str = "data/processed/anomaly_df.parquet"
SCORED_LOGS_PATH: str = "data/processed/scored_logs_df.parquet"

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

# Severity weights.
# HIGH and MEDIUM were added for the multi-section synthetic dataset, whose events
# carry an explicit severity=HIGH|MEDIUM field. They are ordered between the
# pre-existing levels so the CRITICAL > HIGH > ERROR > MEDIUM > WARN > INFO ranking
# is preserved instead of collapsing unknown levels to DEFAULT_SEVERITY_WEIGHT.
SEVERITY_WEIGHTS = {
    "CRITICAL": 1.0,
    "HIGH": 0.85,
    "ERROR": 0.7,
    "MEDIUM": 0.55,
    "WARN": 0.4,
    "INFO": 0.1,
}

DEFAULT_SEVERITY_WEIGHT: float = 0.1


# Counter anomaly proximity
COUNTER_PROXIMITY_WINDOW_SECONDS: int = 30

# Time window (seconds) for joining Section-4 numeric metrics onto event rows
# (features/metric_features.py). A metric sample within ±this of an event is
# considered "near" it. Scoped per scenario so incidents never cross-contaminate.
# NOTE: metric samples are ~480s (8 min) apart, so a 60s window only matches
# events that happen to fall within 60s of a sample (~25% coverage). Widen toward
# ~240s (half the spacing) to lift coverage toward 100%.
METRIC_JOIN_WINDOW_SECONDS: int = 240

# Trailing-window sizes (in metric SAMPLES, not seconds) for the rolling-slope
# trend features. Samples are ~8 min apart, so short=4 ≈ 32 min, long=12 ≈ 96 min.
# Short reacts fast to drift onset but is noisier; long is smoother but laggier.
# Both are computed as backward-looking OLS slope normalised by the series' own
# std, so they share units and are comparable to each other.
METRIC_SLOPE_SHORT_WINDOW: int = 4
METRIC_SLOPE_LONG_WINDOW: int = 12

# ---------------------------------------------------------------------------
# Phase 1 — Parsing
# ---------------------------------------------------------------------------

SESSION_GAP_SECONDS: int = 1800  # 30-min inactivity gap within same host → new session

# Upper bounds on a single session so a steady, never-idle single-host stream
# does not collapse into one multi-day mega-session (which makes the co-occurrence
# graph near-complete and degenerates centrality). A session also closes once it
# spans this long or holds this many events, whichever comes first.
SESSION_MAX_DURATION_SECONDS: int = 900   # 15 min
SESSION_MAX_EVENTS: int = 1000

# All current ingestion is HPE CX switch logs — revisit when multi-device support is added
DEFAULT_SOURCE_TYPE: str = "switch"

# Daemon/process name → canonical subsystem label.
# Only entries where the raw process name is ambiguous or non-standard need to be listed.
# All other names are upper-cased and used as-is (e.g. "OSPF" → "OSPF").
SERVICE_ALIAS_MAP: dict = {
    "eventmgr":    "SYSTEM",
    "hpe-routing": "ROUTING",
    "kernel":      "SYSTEM",
    "sshd":        "SYSTEM",
    "cron":        "SYSTEM",
    "sudo":        "SYSTEM",
    "snmpd":       "SNMP",
    "lldpd":       "LLDP",
    "cfgd":        "CONFIG",

    # --- Generic vendor-neutral component names (mentor's synthetic dataset) ---
    "spanning_tree_daemon":  "STP",
    "redundancy_daemon":     "REDUNDANCY",
    "forwarding_engine":     "FORWARDING",
    "access_control_daemon": "ACL",
    "routing_daemon":        "ROUTING",
    "mac_learning":          "MAC",
    "qos_scheduler_daemon":  "QOS",
    "buffer_manager":        "BUFFER",
    "physical_monitor":      "PHYSICAL",
    "statistics_collector":  "STATS",
    "system_logger":         "SYSTEM",
    "process_monitor":       "SYSTEM",
    "network_monitor":       "NETWORK",

    # --- Routine/heartbeat services: the ~90% baseline noise. Collapsed to a
    #     single NOISE label so the feature stage can suppress/down-weight them. ---
    "monitoring":             "NOISE",
    "continuous_monitoring":  "NOISE",
    "routine_check":          "NOISE",
    "periodic_status":        "NOISE",
    "system_check":           "NOISE",
    "status_verification":    "NOISE",
    "health_check":           "NOISE",
    "metrics_update":         "NOISE",
    "frame_monitoring":       "NOISE",
}

# Canonical service label assigned to routine/heartbeat logs (see SERVICE_ALIAS_MAP).
# Feature/noise-suppression logic can key off this single value.
NOISE_SERVICE_LABEL: str = "NOISE"


# Statistical features
ZSCORE_ROLLING_WINDOW: int = 60
ZSCORE_MIN_STD: float = 1e-6
BURSTINESS_MIN_EVENTS: int = 2

# Feature engineering — zscore baseline
ZSCORE_BASELINE_N_SESSIONS: int = 20  # rolling window: last N sessions per host

# Feature engineering — inter arrival rate
IAR_EMA_ALPHA: float = 0.3  # EMA smoothing factor, per session scope only

# Counter proximity — regex patterns that identify counter/interface anomaly templates
COUNTER_ANOMALY_PATTERNS: list = [
    r"INTERFACE_.*THRESHOLD",
    r"INTERFACE_.*ERROR.*EXCEED",
    r"INTERFACE_.*DROP.*EXCEED",
    r"DROP",
]
# Hint keywords — templates containing these but not matching COUNTER_ANOMALY_PATTERNS
# trigger a WARNING so the pattern list can be updated as new templates are discovered
COUNTER_ANOMALY_HINT_KEYWORDS: list = ["THRESHOLD", "ERROR", "DROP", "EXCEED"]


# Feature pipeline paths
SESSIONIZED_LOGS_PATH: str = (
    "data/processed/sessionized_logs.parquet"
)

FEATURES_OUTPUT_PATH: str = (
    "data/processed/features_df.parquet"
)

# --- Multi-section synthetic dataset artifacts (parsing/synthetic_dataset_loader.py) ---
# Long/tidy numeric metrics extracted from Section 4 of each scenario file.
# Long format means "metric not applicable to a scenario" is simply an absent row,
# avoiding a wide sparse table full of structural NaNs.
METRICS_DF_PATH: str = "data/processed/metrics_df.parquet"

# Per-file ground-truth record from Section 7 (training_label, correlation_signals, …).
# Used ONLY by the evaluation harness as an oracle — never fed to the model.
SCENARIO_LABELS_PATH: str = "data/processed/scenario_labels.parquet"

# ---------------------------------------------------------------------------
# Oracle evaluation (evaluation/oracle_report.py)
# ---------------------------------------------------------------------------

# Log levels that define ground-truth "signal" rows for evaluation.
# Severity is deliberately excluded from IF_FEATURE_COLUMNS, so judging the
# anomaly stage against severity-derived truth is fair (not circular).
# final_score DOES carry a severity term (SCORING_SEVERITY_WEIGHT) — ranking
# metrics computed against this truth are partially favoured by construction.
ORACLE_TRUTH_SEVERITIES: list = ["CRITICAL", "HIGH", "ERROR"]

# Where the oracle evaluation report text file is written.
ORACLE_REPORT_PATH: str = "evaluation/results/oracle_report.txt"

# ---------------------------------------------------------------------------
# Persistent drift detection stores
# ---------------------------------------------------------------------------

# Welford online z-score baseline: one row per (host, template_id).
# Accumulates mean/variance across pipeline runs for cross-run drift detection.
ZSCORE_BASELINE_STORE_PATH: str = "data/processed/zscore_baseline_store.parquet"

# Max session IDs remembered per (host, template_id) for re-run dedup in the
# Welford store. Oldest evicted first (IDs embed the session start timestamp);
# re-running a session older than the cap would double-count it — accepted
# trade-off so the store stays bounded instead of growing forever.
ZSCORE_BASELINE_SEEN_CAP: int = 500

# Rolling feature store for IsolationForest sliding-window retraining.
# Holds raw feature rows from the last FEATURE_ROLLING_MAX_SESSIONS sessions.
FEATURE_ROLLING_STORE_PATH: str = "data/processed/feature_rolling_store.parquet"

# Maximum unique sessions to retain in the rolling feature store.
# Matches RETRAINING_SESSION_WINDOW so the two stores stay in sync.
FEATURE_ROLLING_MAX_SESSIONS: int = 50


# Feature dataframe schema contract
FEATURE_COLUMNS = [
    "sequence_number",
    "session_id",
    "template_id",
    "host",
    "timestamp",
    "frequency_score",
    "burstiness_score",
    "zscore_base",
    "time_delta_prev",
    "time_delta_session_start",
    "inter_arrival_rate",
    "event_weight",
    "counter_proximity",
    # Section-4 numeric-metric features (features/metric_features.py).
    "metric_zscore",
    "metric_zscore_present",
    "drop_rate",
    "drop_rate_present",
    "utilization",
    "utilization_present",
    # Rolling-slope trend features (two windows) for gradual-drift detection.
    "metric_slope_short",
    "metric_slope_short_present",
    "metric_slope_long",
    "metric_slope_long_present",
]

# ---------------------------------------------------------------------------
# Phase 2 — ML Anomaly Detection
# ---------------------------------------------------------------------------

# IsolationForest contamination: expected fraction of anomalies in the dataset.
# 0.05 = 5% anomaly rate assumption. Adjust if first-run anomaly rate looks off.
# If you change this, document it in the JSON sidecar saved alongside the model.
CONTAMINATION: float = 0.05

# A log is flagged is_anomaly=True when combined_score > this threshold.
# 0.5 = middle of [0,1] range; tune upward to reduce false positives.
ANOMALY_THRESHOLD: float = 0.5

# Minimum number of log rows needed before IsolationForest training is attempted.
# Below this, the system falls back to z-score only (cold-start mode).
MIN_TRAIN_SAMPLES: int = 50

# Sliding window retraining: only use logs from the last N sessions.
# Prevents the model from memorising stale historical patterns.
TRAINING_WINDOW_SESSIONS: int = 10

# Periodic retraining trigger: retrain every K new log rows ingested.
# Lower K = fresher model but more compute. Start high, tune down if needed.
RETRAIN_EVERY_K_LOGS: int = 500

# ---------------------------------------------------------------------------
# Phase 3 — IsolationForest hyperparameters (P3: Shreeraksha M)
# ---------------------------------------------------------------------------

# "auto" lets sklearn set contamination to 1/n_estimators; avoids overfitting
# the anomaly fraction assumption on small real-data batches.
IF_CONTAMINATION: str = "auto"

IF_N_ESTIMATORS: int = 100
IF_RANDOM_STATE: int = 42

# Feature columns fed into IsolationForest.
# Identifiers (sequence_number, session_id, host, template_id, timestamp) and
# rule-based signals (event_weight) are excluded — they are not learned features.
IF_FEATURE_COLUMNS: list = [
    "frequency_score",
    "burstiness_score",
    "zscore_base",
    "time_delta_prev",
    "time_delta_session_start",
    "inter_arrival_rate",
    "counter_proximity",
    # Section-4 numeric telemetry — observed measurements (not label proxies),
    # so safe to learn from. Paired *_present flags let the model tell a real 0
    # from a neutrally-filled absent value.
    "metric_zscore",
    "metric_zscore_present",
    "drop_rate",
    "drop_rate_present",
    "utilization",
    "utilization_present",
    # Trend features: backward-looking OLS slope over two windows. These give the
    # model the rate-of-change signal that point-in-time metrics lack — the
    # signature of gradual drift (memory leaks, thermal/CPU creep, disk I/O decay).
    "metric_slope_short",
    "metric_slope_short_present",
    "metric_slope_long",
    "metric_slope_long_present",
]

# Hybrid score weights (IF weighted higher — it captures multi-feature interactions
# that the per-column zscore_base signal misses).
IF_ISOLATION_WEIGHT: float = 0.7
IF_ZSCORE_WEIGHT: float = 0.3

# Model confidence scales linearly 0.0 → 1.0 as training samples grow.
# Below this threshold the blend leans on zscore_base; at or above it the full
# hybrid score is used.
COLD_START_FULL_CONFIDENCE_THRESHOLD: int = 500

# Combined score above this value → is_anomaly = True.
# Used as fallback when score std < 1e-6 (all-identical edge case).
ANOMALY_SCORE_THRESHOLD: float = 0.5

# Multiplier for the dynamic threshold: threshold = mean(scores) + k × std(scores).
# k=2.0 flags scores more than 2 standard deviations above the batch mean (~top 2.3%).
ANOMALY_DYNAMIC_K: float = 1.25

# Anomaly-flag strategy:
#   "absolute"  — flag combined_score > ANOMALY_SCORE_THRESHOLD. Scores are
#                 calibrated against the training distribution (see
#                 _train_model), so this threshold is comparable across runs
#                 and CAN flag nothing on a healthy batch — quantile mode
#                 cannot. Recalibrate the threshold once healthy-day data
#                 exists (see docs/training_data_requirements.md).
#   "quantile"  — flag the top ANOMALY_CONTAMINATION fraction by combined_score.
#                 Self-adjusts to each batch and guarantees a stable, non-zero
#                 anomaly rate — including on fully healthy batches, which is
#                 why it is no longer the default.
#   "dynamic_k" — legacy mean + k·std rule (kept for back-compat). Fragile: when
#                 combined_score is tightly clustered the threshold can exceed the
#                 max achievable score and flag nothing (observed: 0/935).
ANOMALY_FLAG_MODE: str = "absolute"

# Expected fraction of the batch that is anomalous (top-N flagged in quantile mode).
# Was 0.13 ("non-baseline" line rate of the synthetic dataset) — the oracle
# harness measured the true severity-signal rate at ~2.1%, so 0.13 flagged ~6x
# too many rows (precision 0.02). Kept near the true rate for quantile mode.
ANOMALY_CONTAMINATION: float = 0.03

# Sliding window: retrain on the last N sessions only.
RETRAINING_SESSION_WINDOW: int = 50

# Periodic trigger: retrain every K new log rows ingested.
RETRAINING_TRIGGER_EVERY_K: int = 1000

# Directory where versioned model pkl files and JSON sidecars are stored.
MODEL_STORE_PATH: str = "ml/model_store"

# ---------------------------------------------------------------------------
# Phase 4 — Importance Scoring (P4: Ujwal Hegde)
# ---------------------------------------------------------------------------

# Weights for the final importance score — 3-term formula. Weights sum to 1.0.
#   final_score = ML_WEIGHT·combined_score        (behavioral anomaly, unsupervised IF)
#               + GRAPH_WEIGHT·centrality_score    (structural importance)
#               + SEVERITY_WEIGHT·event_weight     (declared severity)
# Severity is kept as its own explicit, tunable term here rather than baked into the
# IsolationForest features. This avoids (a) leaking the severity label into an
# unsupervised model that is then validated against severity-derived ground truth,
# and (b) double-counting severity once the model and the score both carry it.
SCORING_ML_WEIGHT: float = 0.5
SCORING_GRAPH_WEIGHT: float = 0.25
SCORING_SEVERITY_WEIGHT: float = 0.25

# Label thresholds: ignore / low / medium / critical
# Retuned 2026-06-11 against the oracle report on the 7-day synthetic dataset:
# the old (0.2/0.5/0.75) boundaries sat above the entire score distribution —
# 0 rows ever reached "critical" and noise suppression was ~0. Current values
# are anchored to the measured distribution (signal p1≈0.25, median≈0.31):
# capture(med+)=0.55, signal ignored=0.3%, noise suppression=0.92.
# Recalibrate alongside ANOMALY_SCORE_THRESHOLD once healthy-day data exists.
#
# 2026-06-18: validated against 3 generated clean/healthy days (no anomalies).
# Per-batch normalisation in the anomaly detector stretches benign variance
# across [0,1], so the "most unusual" benign lines on a healthy day reached
# final_score≈0.60 and crossed the old 0.50 critical cutoff (false positives).
# Raised the critical cutoff above the measured clean-day max (0.603) so a
# healthy batch yields zero criticals. NOTE: this is a threshold band-aid — the
# root cause is relative scoring + absolute labels (see config:418); medium
# remains noisy on clean data.
LABEL_IGNORE_MAX: float = 0.25
LABEL_LOW_MAX: float = 0.30
LABEL_MEDIUM_MAX: float = 0.65
# Anything above LABEL_MEDIUM_MAX → critical

# ---------------------------------------------------------------------------
# Message-aware score adjustments (scoring/importance_scorer.py)
# ---------------------------------------------------------------------------
# The synthetic dataset tags severity inconsistently: recovery / routine
# all-clear lines often inherit an incident block's HIGH/CRITICAL log_level
# (inflating the severity term), while the real onset markers
# ("<SCENARIO> event in progress - monitoring") are sometimes tagged INFO.
# These two message-text rules correct the ranking at the scoring layer without
# retraining the model.
#
# NOTE: the patterns below are tuned to THIS synthetic dataset's wording. On
# real HPE logs, replace them with the actual recovery/onset vocabulary — the
# mechanism generalizes, the specific word list does not.

# (1) Damp recovery / all-clear lines so they fall out of the high tiers.
RECOVERY_SCORE_DAMPING: float = 0.45
RECOVERY_MESSAGE_PATTERNS: list = [
    r"\bresolved\b", r"\brestored\b", r"\brecovered\b", r"\bcleared\b",
    r"\bmitigat", r"restart complete", r"back to normal",
    r"\bhealthy\b", r"\bstable\b", r"\bnominal\b", r"\boperational\b",
    r"within acceptable", r"no unresolved", r"no violations",
    r"no unauthorized", r"audit passed", r"check passed",
    r"telemetry exported", r"statistics collection", r"performance report",
    # Residual benign-status phrases confirmed across the May/June batches.
    # Kept specific so they don't catch recovery-flavoured *true* detections
    # such as "ARP rate returning to normal" or "MAC table ... reduced after
    # aging" (those carry HIGH severity and correctly flag the incident window).
    r"sync state synced", r"peer reachable", r"convergence complete",
    r"no blackholes", r"no recurrence", r"no issues found",
    r"within normal bounds", r"established, 0 down", r"operationally up",
    r"maintenance check",
]

# (2) Floor genuine onset markers so they stay visible even when mis-tagged
#     INFO. CRITICAL-level onsets already score higher; this only lifts
#     under-scored ones into the medium tier (0.30–0.50). Recovery and onset
#     sets are disjoint — recovery phrases never contain "event in progress".
ONSET_MARKER_PATTERN: str = r"event in progress"
ONSET_SCORE_FLOOR: float = 0.45

# ---------------------------------------------------------------------------
# Severity credibility gate (scoring/importance_scorer.py)
# ---------------------------------------------------------------------------
# A log line whose MESSAGE asserts normal / healthy operation must not earn the
# severity term's importance bonus just because its severity TAG says CRITICAL.
# Both the synthetic generator and real vendor logs routinely emit benign status
# lines at an inflated log_level — e.g. "ASIC temperature: 50C - NORMAL
# (threshold: 55C)" carried at severity=CRITICAL. Left unchecked, those lines
# are the ONLY rows that reach the "critical" label (verified 2026-06-22 on
# clean_days + anomaly_days), while the genuine incidents sit in "medium".
#
# The gate reverts a contradicted line's severity contribution to the INFO
# baseline (DEFAULT_SEVERITY_WEIGHT), so it must earn any score from the ML /
# graph signal instead of a label the message itself contradicts. It only
# REMOVES an unearned bonus — it never lowers a line below its ML/graph score,
# so it cannot suppress a true detection.
#
# Generalizes across datasets; the list below is a starting lexicon of normalcy
# assertions — tune per vendor, same caveat as RECOVERY_MESSAGE_PATTERNS.
SEVERITY_GATE_ENABLED: bool = True
SEVERITY_GATE_BENIGN_PATTERNS: list = [
    r"-\s*normal\b", r"\bnominal\b", r"\bhealthy\b",
    r"within bounds", r"within limits", r"within normal",
    r"no alerts", r"no anomal", r"no errors", r"no issues",
    r"check passed", r"all clear",
]

# ---------------------------------------------------------------------------
# Component event-rate drift signal (scoring/drift_scorer.py)
# ---------------------------------------------------------------------------
# The IsolationForest features are point-in-time / burstiness based, so a
# GRADUAL failure that emits one log every ~40s (e.g. an OOM kill cascade
# spread over 17 minutes) has no burst signature and is missed entirely
# (verified 2026-06-22). This signal catches it deterministically, without the
# model: bin each run into fixed windows, count events per component per bin,
# and z-score every bin against that component's OWN full-day baseline (silent
# bins included). A normally-quiet component that suddenly chatters — whether a
# 20-event burst or a slow drip over many bins — lands in high-z bins.
#
# Self-calibrating per component, so it needs no per-dataset tuning and is
# robust to cold-start and to training-set contamination. Only the ELEVATED
# direction counts (a quiet stretch is not a drift anomaly). It is an additive
# corroborating term on final_score; the existing ML/graph/severity weights are
# left unchanged (this adds evidence, it does not re-fit the blend).
# DISABLED BY DEFAULT. Empirically (2026-06-22) a WITHIN-FILE per-component
# baseline cannot separate these anomalies from clean-day chatter: the clean day
# reaches MEMORY_MANAGER rate-ratio 3.0, while the real PROTOCOL_STARVATION's
# component (stp_state, already high-rate) hits only 1.14 and OOM's
# MEMORY_MANAGER (active all day via the leak) only 3.0 — i.e. the anomaly's own
# component spikes LESS than normal periodic noise. A loose threshold floods the
# clean day with false criticals; a tight one misses the anomalies. The signal
# the model needs is a CROSS-DAY baseline (this component's rate today vs the
# same component on known-clean days) — which requires the clean-baseline corpus.
# The code below is the foundation for that; left off until the baseline exists
# so it can't ship as a knob tuned to one dataset.
SCORING_DRIFT_ENABLED: bool = False
SCORING_DRIFT_WEIGHT: float = 0.40
DRIFT_BIN_SECONDS: int = 120
DRIFT_MIN_COMPONENT_EVENTS: int = 8      # need history before trusting a rate
DRIFT_MIN_ACTIVE_BINS: int = 3           # need a typical-rate baseline to beat
# Drift fires on a bin whose count exceeds the component's TYPICAL active-bin
# rate (median over non-zero bins) by a ratio. Comparing against the typical
# ACTIVE rate — not against the zero-filled baseline — is what stops normal
# periodic components (steady ~1/bin) from lighting up. Ramps 0→1 between MIN
# and FULL multiples of that median.
DRIFT_RATIO_MIN: float = 3.0             # <3× median active rate → no drift
DRIFT_RATIO_FULL: float = 8.0            # ≥8× median active rate → full drift

# Incident clustering (incident_clusterer.py)
# ------------------------------------------------------------------
# Anomaly-SEEDED temporal windowing. The old approach (DBSCAN over
# [final_score, centrality, temporal_proximity]) clustered by score-similarity,
# which (a) merged the dense benign mass into day-spanning blobs and (b) dropped
# the rare high-score anomalies as DBSCAN noise — verified 2026-06-22: 0/21
# critical rows landed in any incident, incidents spanned up to 22.9h.
#
# New approach: seed only on the "interesting" rows (anomalous / high-score /
# high-severity), then group seeds that are within INCIDENT_WINDOW_SECONDS of
# each other in ABSOLUTE time. Gaps are measured between SEEDS (sparse), not all
# logs, so continuous background noise can't bridge incidents and a real
# incident stays a minutes-long burst. Groups with < INCIDENT_MIN_SEEDS seeds
# are dropped, so isolated false-positive seeds on clean days form no incident.
# Seeding: a row is a seed if it is anomalous OR carries a non-trivial label OR
# is high severity. The real anomaly bursts (PROTOCOL_STARVATION, SPLIT_BRAIN)
# are DENSE clusters of `medium` rows whose final_score sits below 0.5, so the
# per-row score is NOT the discriminator — DENSITY is. Seed broadly on label,
# then let INCIDENT_MIN_SEEDS reject scattered clean-day medium noise: a real
# incident packs many seeds into one window; clean noise spreads ~1 per window.
INCIDENT_WINDOW_SECONDS: int = 180      # consecutive seeds within this → same incident
INCIDENT_MIN_SEEDS: int = 10            # density floor: fewer seeds in window → not an incident
INCIDENT_SEED_LABELS: tuple = ("medium", "critical")  # labels that seed
INCIDENT_SEED_SCORE_MIN: float = 0.50   # final_score at/above this also seeds
INCIDENT_SEED_SEVERITY_MIN: float = 0.70  # event_weight at/above this (ERROR+) also seeds

# Legacy DBSCAN knobs — retained for backward-compat / fallback only.
DBSCAN_EPS: float = 0.08
DBSCAN_MIN_SAMPLES: int = 3
INCIDENT_MAX_GAP_SECONDS: int = 900  # 15 minutes

# Root cause candidates selected per incident cluster.
ROOT_CAUSE_TOP_N: int = 3

# Missing upstream input fill strategy.
# Rows absent from anomaly_df or graph_scores_df after the left join are
# filled with the column mean of the non-null rows. Boolean columns
# (is_anomaly, in_graph, in_sequence) are always filled with False.
MISSING_INPUT_FILL: str = "mean"

# Hard cap on the fraction of rows allowed to be missing from an upstream
# input before scoring FAILS instead of mean-filling. Mean-filling makes a
# missing row look perfectly average — the most dangerous disguise for rows
# that were dropped upstream precisely because something was wrong with them.
# A few stragglers are tolerable; a systematic gap is a pipeline bug.
# Rows that were filled are flagged in the anomaly_missing / graph_missing
# output columns either way.
SCORING_MAX_MISSING_FRACTION: float = 0.05

# ---------------------------------------------------------------------------
# Phase 5.5 — Cross-Run Incident Correlation
# ---------------------------------------------------------------------------

# Master switch: set False to skip P5.5 entirely (no history written or read).
CROSS_RUN_ENABLED: bool = True

# How far back (hours) to search the incident history for potential precursors.
# 72h = 3 days covers weekend-to-Monday drift and slow-burn memory leaks.
CROSS_RUN_LOOKBACK_HOURS: int = 72

# Jaccard similarity threshold for declaring two incidents "related".
# 0.3 = at least 30% of the combined template vocabulary must be shared.
# Intentionally low: precursors typically share a subset, not all, templates.
CROSS_RUN_SIMILARITY_THRESHOLD: float = 0.3

# Minimum Jaccard similarity floor. Even if overlap_coefficient is high,
# the link is rejected if Jaccard similarity is below this floor.
# Prevents a 1-template incident from linking to a 100-template incident.
CROSS_RUN_MIN_JACCARD: float = 0.05

# Score boost applied to precursor logs when a descendant critical incident
# is discovered. Capped to [0, 1] after application.
# elevated_score = min(1.0, original_score + PRECURSOR_BOOST * chain_confidence)
PRECURSOR_BOOST: float = 0.15

# Prefix for generated chain IDs (format: CHAIN-<unix_ts>-<seq>).
CHAIN_ID_PREFIX: str = "CHAIN"

# Parquet-based fallback store for incident history.
# Used in dry-run mode (no Postgres) and synced to the DB on live runs.
INCIDENT_HISTORY_PATH: str = "data/processed/incident_history.parquet"
