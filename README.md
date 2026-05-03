# Network Log Analysis ML Pipeline

This project is a comprehensive Machine Learning pipeline for network log analysis. It covers the full lifecycle from data ingestion to anomaly detection, correlation, log importance scoring, data storage, and visualization.

## Development Progress

| Module | Status | Notes |
|---|---|---|
| `common/config.py` | Done | Lazy env-var access + graph tuning constants |
| `common/env_handler.py` | Done | Fail-fast `.env` loader |
| `correlation/graph_builder.py` | Done | Co-occurrence graph, anomaly linkage, node cap |
| `correlation/tests/test_correlation.py` | Done | 23 unit tests, all passing |
| `ingestion/` | Skeleton | Not yet implemented |
| `parsing/` | Skeleton | Not yet implemented |
| `features/` | Skeleton | Not yet implemented |
| `ml/` | Skeleton | Not yet implemented |
| `scoring/` | Skeleton | Importance scoring module initialized (config + stubs) |
| `storage/` | Skeleton | Not yet implemented |
| `visualization/` | Skeleton | Not yet implemented |
| `evaluation/` | Skeleton | Not yet implemented |

## Architecture & Modules

The project is structured into logical modules reflecting the steps in the ML pipeline:

- **`data/`**: Directory for storing raw and processed datasets.
- **`ingestion/`**: Data ingestion processes to bring in network logs.
- **`parsing/`**: Parsers to convert raw log lines into structured data formats.
- **`features/`**: Feature engineering logic to transform parsed data into model-ready features.
- **`ml/`**: Machine Learning models specifically built for detecting anomalies in log data.
- **`correlation/`**: Identifies correlations between log events and anomalies. See details below.
- **`scoring/`**: Mechanisms to compute an "importance score" for log events to prioritize review.
- **`storage/`**: Interfaces for data persistence, interacting with PostgreSQL and Elasticsearch.
- **`visualization/`**: Configurations and instructions for dashboarding via Kibana and Grafana.
- **`evaluation/`**: Contains scripts and utilities for evaluating model and pipeline performance.
- **`pipeline.py`**: The central orchestrator script that ties the modules together.

## Correlation Module

### Graph Schema

The correlation graph is a weighted undirected graph where:

- **Node** = a unique log template string (from the parsing stage) or an anomaly marker
  - `id`: template string or `anomaly:<label>`
  - `node_type`: `"log_template"` or `"anomaly"`
  - `count`: raw occurrence frequency
- **Edge** = co-occurrence within a configurable time window
  - `co_occurrences`: raw count of windows where both nodes appear together
  - `weight`: normalized value in `(0, 1]`, where `1.0` is the most frequent co-occurring pair

### Configuration (`common/config.py`)

| Constant | Default | Description |
|---|---|---|
| `CORRELATION_TIME_WINDOW_SECONDS` | `60` | Window width for co-occurrence detection |
| `MAX_GRAPH_NODES` | `500` | Cap on template nodes; anomaly nodes are always admitted |

### Running the Correlation Module

**Unit tests:**

```bash
python -m pytest correlation/tests/test_correlation.py -v
```

**Manual inspection (synthetic 5-node example):**

```bash
python3 -m correlation.manual_test
```

## Infrastructure Setup

To run the pipeline infrastructure, we use Docker Compose to spin up the required databases and visualization tools.

### 1. Setup Virtual Environment and Dependencies

Create a virtual environment and install the required libraries:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Set up your local environment variables:

```bash
cp .env.example .env
```

### 3. Start the Services

Deploy the infrastructure stack:

```bash
docker compose up -d
```

### 4. Running Scripts

To avoid import errors, **always run scripts from the project root** using the `-m` (module) flag:

```bash
python3 -m storage.db_writer
```

## Environment Variable Handling

All environment variables must be accessed using the global environment handler via `common.config`.

### Usage

```python
# Variables are lazily evaluated. They are only checked when actually imported/used.
from common.config import DB_URL
```

### Behavior

- **Lazy Evaluation**: Fails only when a specific variable is accessed.
- **Clean Errors**: If a variable is missing, the app outputs a clear message and exits without a long traceback:
  `[ENV ERROR] Missing required variable: NAME`

## Rules

- Do not use `os.getenv()` or `dotenv` directly.
- Always import variables from `common.config`.
- Never print raw environment variables to the console.
- Always run scripts from the project root using `python3 -m <module>` to avoid import errors.

## Scoring Module (Importance Scoring)

The `scoring/` module computes a final importance score for each log by combining signals from multiple components in the pipeline.

### Inputs

- `anomaly_df` (from ML module)
- `graph_scores_df` (from correlation module)
- `features_df` (from feature engineering module)

### Scoring Configuration (`common/config.py`)

| Parameter | Default | Description |
|----------|--------|-------------|
| ML_WEIGHT | 0.4 | Contribution of ML anomaly signal |
| GRAPH_WEIGHT | 0.35 | Contribution of correlation/graph signal |
| RULE_WEIGHT | 0.25 | Contribution of rule-based features |

### Label Thresholds

| Label | Range |
|------|------|
| ignore | 0.0 – 0.2 |
| low | 0.2 – 0.5 |
| medium | 0.5 – 0.75 |
| critical | 0.75 – 1.0 |

### Clustering Configuration (DBSCAN)

| Parameter | Default | Description |
|----------|--------|-------------|
| DBSCAN_EPS | 0.5 | Maximum distance between samples in a cluster |
| DBSCAN_MIN_SAMPLES | 5 | Minimum number of logs required to form a cluster |

DBSCAN will be used in later phases to group related logs into incidents.

### Output (`scored_logs_df`)

The module will output a DataFrame with the following schema:

- `log_id`
- `final_score`
- `label`
- `incident_id`
- `is_root_cause`
- `root_cause_confidence`

The output will be saved to:

```text
data/processed/scored_logs_df.parquet