# Network Log Analysis ML Pipeline

This project is a comprehensive Machine Learning pipeline for network log analysis. It covers the full lifecycle from data ingestion to anomaly detection, correlation, log importance scoring, data storage, and visualization.

## Development Progress

| Module                                  | Status      | Notes                                                           |
| --------------------------------------- | ----------- | --------------------------------------------------------------- |
| `common/config.py`                      | Done        | Lazy env-var access + graph tuning constants                    |
| `common/env_handler.py`                 | Done        | Fail-fast `.env` loader                                         |
| `correlation/graph_builder.py`          | Done        | Co-occurrence graph, anomaly linkage, node cap                  |
| `correlation/tests/test_correlation.py` | Done        | 23 unit tests, all passing                                      |
| `ingestion/`                            | Skeleton    | Not yet implemented                                             |
| `parsing/`                              | Skeleton    | Not yet implemented                                             |
| `features/`                             | Skeleton    | Not yet implemented                                             |
| `ml/`                                   | In Progress | Isolation Forest + Z-score hybrid anomaly detection implemented |
| `scoring/` | Done | Full importance scoring, incident clustering, and root-cause analysis pipeline implemented |                                          |
| `storage/`                              | Skeleton    | Not yet implemented                                             |
| `visualization/`                        | Skeleton    | Not yet implemented                                             |
| `evaluation/`                           | Skeleton    | Not yet implemented                                             |

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

| Constant                          | Default | Description                                              |
| --------------------------------- | ------- | -------------------------------------------------------- |
| `CORRELATION_TIME_WINDOW_SECONDS` | `60`    | Window width for co-occurrence detection                 |
| `MAX_GRAPH_NODES`                 | `500`   | Cap on template nodes; anomaly nodes are always admitted |

### Running the Correlation Module

**Unit tests:**

```bash
python -m pytest correlation/tests/test_correlation.py -v
```

**Manual inspection (synthetic 5-node example):**

```bash
python3 -m correlation.manual_test
```

## ML Module (Anomaly Detection)

### Overview

The ML module detects anomalies using:

- Isolation Forest (ML-based)
- Z-score (statistical)

### Model Design

- Isolation Forest → learns normal behavior
- Z-score → measures deviation

Hybrid:

```text
combined_score = w1 * isolation_score + w2 * zscore
```

where:

- w1 = weight for ML signal
- w2 = weight for statistical signal

### Configuration (common/config.py)

| Parameter                | Description                       |
| ------------------------ | --------------------------------- |
| contamination            | Expected proportion of anomalies  |
| weight_isolation         | Weight for Isolation Forest score |
| weight_zscore            | Weight for Z-score contribution   |
| training_window_sessions | Number of sessions for retraining |

### Output Contract

| Column          | Description            |
| --------------- | ---------------------- |
| log_id          | Unique identifier      |
| isolation_score | Isolation Forest score |
| zscore          | Statistical deviation  |
| combined_score  | Hybrid score           |
| is_anomaly      | Boolean anomaly flag   |

Saved to:

```text
data/processed/anomaly_df.parquet
```

### Model Training

Handled via `ml/trainer.py`:

- Trains Isolation Forest
- Saves model:

```text
ml/model_store/isolation_forest_v{timestamp}.pkl
```

### Cold-Start Strategy

If insufficient data:

- predictions unstable
- fallback:
  - rely on z-score
  - or mark non-anomalous

### Testing

```bash
pytest ml/tests/test_anomaly.py
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

The `scoring/` module integrates outputs from:
- feature engineering
- ML anomaly detection
- graph correlation

to generate:
- final importance scores
- severity labels
- incident clusters
- root-cause candidates

### Components

#### `importance_scorer.py`
- Loads:
  - `features_df.parquet`
  - `anomaly_df.parquet`
  - `graph_scores_df.parquet`
- Merges inputs on `log_id`
- Handles missing ML/graph outputs using fallback scores (`0.0`)
- Computes:

```text
final_score =
    (ML_WEIGHT * combined_score)
  + (GRAPH_WEIGHT * centrality_score)
  + (RULE_WEIGHT * severity_weight)
```

- Saves:
  - `data/processed/scored_logs_df.parquet`

---

#### `label_mapper.py`
Maps `final_score` into:
- `ignore`
- `low`
- `medium`
- `critical`

using thresholds defined in `common/config.py`.

---

#### `incident_clusterer.py`
- Uses DBSCAN clustering
- Clustering features:
  - `final_score`
  - `centrality_score`
  - `time_delta_session_start`
- Assigns incident IDs:
  - `INC-000`
  - `INC-001`
- Noise points receive:
  - `incident_id = None`

### DBSCAN Configuration

| Parameter | Default |
|-----------|----------|
| `DBSCAN_EPS` | `0.5` |
| `DBSCAN_MIN_SAMPLES` | `5` |

---

#### `root_cause_engine.py`
- Identifies top root-cause candidates per incident
- Ranks logs using `centrality_score`
- Computes:
  - `root_cause_confidence`
- Saves:
  - `data/processed/root_causes_df.parquet`

---

## Output Contracts

### `scored_logs_df.parquet`

Schema:
- `log_id`
- `final_score`
- `label`
- `incident_id`
- `is_root_cause`
- `root_cause_confidence`

### `root_causes_df.parquet`

Schema:
- `incident_id`
- `root_cause_log_id`
- `confidence_score`

---

## Tests

Implemented in:

```text
scoring/tests/test_scoring.py
```

Run using:

```bash
pytest scoring/tests/test_scoring.py -v
```