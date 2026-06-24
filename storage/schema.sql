-- =========================
-- LOGS TABLE
-- =========================
-- run_id (format YYYYMMDD, the batch's earliest event date) makes the
-- per-batch sequence_number globally unique so successive upload batches
-- ACCUMULATE instead of overwriting each other on (sequence_number) collision.
-- NOTE: the composite-key change only applies to a FRESH database — reset with
-- `docker compose down -v` before relying on it.
CREATE TABLE IF NOT EXISTS logs (
    run_id VARCHAR(20) NOT NULL DEFAULT 'legacy',
    sequence_number BIGINT,
    PRIMARY KEY (run_id, sequence_number),
    timestamp TIMESTAMPTZ,
    source_type VARCHAR(100),
    service VARCHAR(100),
    host VARCHAR(100),
    log_level VARCHAR(50),
    event_type VARCHAR(100),
    event_action VARCHAR(100),
    template_id VARCHAR(100),
    frequency INT,
    event_weight DOUBLE PRECISION,
    message TEXT,
    metadata JSONB,
    session_id TEXT,

    -- Provenance / inference-tracking columns (synthetic_dataset_loader.py)
    source_file VARCHAR(255),
    scenario_id VARCHAR(255),
    section INT,
    component VARCHAR(100),
    code_location VARCHAR(255),
    severity_explicit BOOLEAN,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotent upgrades for pre-existing logs tables (CREATE IF NOT EXISTS above
-- is a no-op when the table already exists, so add new columns explicitly).
ALTER TABLE logs ADD COLUMN IF NOT EXISTS source_file VARCHAR(255);
ALTER TABLE logs ADD COLUMN IF NOT EXISTS scenario_id VARCHAR(255);
ALTER TABLE logs ADD COLUMN IF NOT EXISTS section INT;
ALTER TABLE logs ADD COLUMN IF NOT EXISTS component VARCHAR(100);
ALTER TABLE logs ADD COLUMN IF NOT EXISTS code_location VARCHAR(255);
ALTER TABLE logs ADD COLUMN IF NOT EXISTS severity_explicit BOOLEAN;

-- =========================
-- FEATURES TABLE
-- =========================
CREATE TABLE IF NOT EXISTS features (
    run_id VARCHAR(20) NOT NULL DEFAULT 'legacy',
    sequence_number BIGINT,
    PRIMARY KEY (run_id, sequence_number),
    FOREIGN KEY (run_id, sequence_number)
        REFERENCES logs(run_id, sequence_number) ON DELETE CASCADE,
    session_id TEXT,
    template_id VARCHAR(100),
    host VARCHAR(100),
    timestamp TIMESTAMPTZ,
    frequency_score DOUBLE PRECISION,
    burstiness_score DOUBLE PRECISION,
    zscore_base DOUBLE PRECISION,
    time_delta_prev DOUBLE PRECISION,
    time_delta_session_start DOUBLE PRECISION,
    inter_arrival_rate DOUBLE PRECISION,
    event_weight DOUBLE PRECISION,
    counter_proximity DOUBLE PRECISION,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =========================
-- ANOMALIES TABLE
-- =========================
CREATE TABLE IF NOT EXISTS anomalies (
    run_id VARCHAR(20) NOT NULL DEFAULT 'legacy',
    sequence_number BIGINT,
    PRIMARY KEY (run_id, sequence_number),
    FOREIGN KEY (run_id, sequence_number)
        REFERENCES logs(run_id, sequence_number) ON DELETE CASCADE,
    isolation_score DOUBLE PRECISION,
    zscore_norm DOUBLE PRECISION,
    combined_score DOUBLE PRECISION,
    is_anomaly BOOLEAN,
    model_confidence DOUBLE PRECISION,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =========================
-- SCORES TABLE
-- =========================
CREATE TABLE IF NOT EXISTS scores (
    run_id VARCHAR(20) NOT NULL DEFAULT 'legacy',
    sequence_number BIGINT,
    PRIMARY KEY (run_id, sequence_number),
    FOREIGN KEY (run_id, sequence_number)
        REFERENCES logs(run_id, sequence_number) ON DELETE CASCADE,
    final_score DOUBLE PRECISION,
    label TEXT,
    -- correlation_id is globalized to INC-<run_id>-<NNNN> so it stays unique
    -- across batches and matches incidents.incident_id.
    correlation_id VARCHAR(100),
    is_root_cause BOOLEAN,
    root_cause_confidence DOUBLE PRECISION,
    is_cross_system BOOLEAN,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =========================
-- INCIDENTS TABLE
-- =========================
CREATE TABLE IF NOT EXISTS incidents (
    -- incident_id is globalized to INC-<run_id>-<NNNN>, unique across batches.
    incident_id TEXT PRIMARY KEY,
    run_id VARCHAR(20) NOT NULL DEFAULT 'legacy',
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    severity TEXT,
    label TEXT,
    root_cause_confidence DOUBLE PRECISION,
    log_count INTEGER,
    status TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =========================
-- SUMMARIES TABLE
-- =========================
CREATE TABLE IF NOT EXISTS summaries (
    correlation_id TEXT PRIMARY KEY,
    summary_text TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_summaries_correlation_id
ON summaries (correlation_id);

-- =========================
-- INDEXES (LOGS)
-- =========================
CREATE INDEX IF NOT EXISTS idx_logs_sequence_number ON logs (sequence_number);
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs (timestamp);
CREATE INDEX IF NOT EXISTS idx_logs_log_level ON logs (log_level);
CREATE INDEX IF NOT EXISTS idx_logs_host ON logs (host);
CREATE INDEX IF NOT EXISTS idx_logs_template_id ON logs (template_id);

-- =========================
-- INDEXES (FEATURES)
-- =========================
CREATE INDEX IF NOT EXISTS idx_features_sequence_number ON features (sequence_number);
CREATE INDEX IF NOT EXISTS idx_features_session_id ON features (session_id);
CREATE INDEX IF NOT EXISTS idx_features_timestamp ON features (timestamp);

-- =========================
-- INDEXES (ANOMALIES)
-- =========================
CREATE INDEX IF NOT EXISTS idx_anomalies_sequence_number ON anomalies (sequence_number);
CREATE INDEX IF NOT EXISTS idx_anomalies_is_anomaly ON anomalies (is_anomaly);

-- =========================
-- INDEXES (SCORES)
-- =========================
CREATE INDEX IF NOT EXISTS idx_scores_sequence_number ON scores (sequence_number);
CREATE INDEX IF NOT EXISTS idx_scores_label ON scores (label);
CREATE INDEX IF NOT EXISTS idx_scores_correlation_id ON scores (correlation_id);
CREATE INDEX IF NOT EXISTS idx_scores_is_root_cause ON scores (is_root_cause);

-- =========================
-- INDEXES (INCIDENTS)
-- =========================
CREATE INDEX IF NOT EXISTS idx_incidents_incident_id ON incidents (incident_id);
CREATE INDEX IF NOT EXISTS idx_incidents_start_time ON incidents (start_time);
CREATE INDEX IF NOT EXISTS idx_incidents_label ON incidents (label);

-- =========================
-- SCORES TABLE — chain columns (P5.5)
-- Added via ALTER so existing databases are upgraded safely.
-- =========================
ALTER TABLE scores ADD COLUMN IF NOT EXISTS chain_id TEXT;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS precursor_incident_id TEXT;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS chain_position INT;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS chain_confidence DOUBLE PRECISION;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS is_precursor_elevated BOOLEAN DEFAULT FALSE;

-- =========================
-- INCIDENT HISTORY TABLE (P5.5)
-- One row per incident per pipeline run.  Globally unique incident_id uses
-- format INC-<YYYYMMDD>-<seq> to avoid collisions across runs.
-- =========================
CREATE TABLE IF NOT EXISTS incident_history (
    incident_id              TEXT PRIMARY KEY,
    run_date                 DATE NOT NULL,
    run_timestamp            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    start_time               TIMESTAMPTZ,
    end_time                 TIMESTAMPTZ,
    template_fingerprint     TEXT NOT NULL,
    root_cause_templates     TEXT,
    severity                 TEXT,
    is_escalated             BOOLEAN DEFAULT TRUE,
    escalation_reason        TEXT,
    n_critical_rows          INT DEFAULT 0,
    n_high_severity_rows     INT DEFAULT 0,
    log_count                INT,
    hosts                    TEXT,
    is_cross_system          BOOLEAN DEFAULT FALSE,
    chain_id                 TEXT,
    precursor_incident_id    TEXT,
    chain_position           INT,
    is_precursor_elevated    BOOLEAN DEFAULT FALSE,

    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_incident_history_incident_id
    ON incident_history (incident_id);
CREATE INDEX IF NOT EXISTS idx_incident_history_end_time
    ON incident_history (end_time);
CREATE INDEX IF NOT EXISTS idx_incident_history_chain_id
    ON incident_history (chain_id);
CREATE INDEX IF NOT EXISTS idx_incident_history_run_date
    ON incident_history (run_date);

-- Incident escalation gate columns. Idempotent migration so existing databases
-- (created before the gate) gain the columns on the next apply_schema, without a
-- drop. CREATE TABLE IF NOT EXISTS alone never alters an existing table.
ALTER TABLE incident_history ADD COLUMN IF NOT EXISTS is_escalated         BOOLEAN DEFAULT TRUE;
ALTER TABLE incident_history ADD COLUMN IF NOT EXISTS escalation_reason    TEXT;
ALTER TABLE incident_history ADD COLUMN IF NOT EXISTS n_critical_rows      INT DEFAULT 0;
ALTER TABLE incident_history ADD COLUMN IF NOT EXISTS n_high_severity_rows INT DEFAULT 0;