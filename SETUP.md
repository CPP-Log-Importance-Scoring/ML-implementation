# Setup & Integration Guide

A step-by-step guide to running the **Log Importance Scoring & Cross-Signal Correlation**
pipeline and dashboard in a brand-new environment.

If you have never seen this repository before, start here and follow the steps in order.
For architecture and design internals, see [`README.md`](README.md).

---

## Table of Contents

1. [What this tool does](#1-what-this-tool-does)
2. [Prerequisites](#2-prerequisites)
3. [Clone the repository](#3-clone-the-repository)
4. [Configure environment variables](#4-configure-environment-variables)
5. [Install Python dependencies](#5-install-python-dependencies)
6. [Start the backing services](#6-start-the-backing-services-postgres--elasticsearch)
7. [Run the ML pipeline](#7-run-the-ml-pipeline)
8. [Launch the dashboard](#8-launch-the-dashboard)
9. [Running with your own logs](#9-running-with-your-own-logs)
10. [Verifying it worked](#10-verifying-it-worked)
11. [Alternative: run everything in Docker](#11-alternative-run-everything-in-docker)
12. [Generating sample data](#12-generating-sample-data)
13. [Troubleshooting](#13-troubleshooting)
14. [Deployment considerations](#14-deployment-considerations)

---

## 1. What this tool does

It ingests raw network-switch logs and, in one pass:

1. **Parses** them into reusable message *templates* and per-host *sessions*
2. **Extracts features** (burst rate, z-score, timing, severity, metric trends)
3. **Detects anomalies** with a hybrid IsolationForest + z-score model
4. **Correlates** events into a co-occurrence graph (centrality, sequences)
5. **Scores & labels** each line, groups them into **incidents**, and identifies a **root cause**
6. **Links incidents across runs** into causal chains
7. **Persists** results to PostgreSQL + Elasticsearch and serves a **Streamlit dashboard**

**Net effect:** thousands of noisy log lines become a handful of ranked, explainable incidents.

---

## 2. Prerequisites

Install these before anything else.

| Requirement | Version | Why | Check with |
|---|---|---|---|
| **Python** | 3.11 (recommended) | Runs the pipeline & dashboard | `python --version` |
| **Docker Desktop** | any recent | Runs PostgreSQL 15 + Elasticsearch 8.11 | `docker --version` |
| **Git** | any recent | Clone the repo | `git --version` |

> **Note on Python version:** the containers are built on `python:3.11-slim`. Python 3.10–3.12
> generally work locally, but 3.11 is the validated version.

**Optional but recommended:**
- A free **Groq API key** (for AI incident summaries) — get one at <https://console.groq.com/keys>.
  Without it, everything still runs; only the "AI Incident Summary" panel stays empty.

**Ports used** — make sure these are free:

| Port | Service |
|---|---|
| `5432` | PostgreSQL |
| `9200` | Elasticsearch |
| `8501` | Streamlit dashboard |

---

## 3. Clone the repository

```bash
git clone https://github.com/CPP-Log-Importance-Scoring/ML-implementation.git
cd ML-implementation
```

> **Important:** always run commands from the **project root** (the folder containing
> `pipeline.py`). Running from a subfolder causes import errors.

---

## 4. Configure environment variables

Copy the template and fill it in:

**macOS / Linux**
```bash
cp .env.example .env
```

**Windows (PowerShell)**
```powershell
Copy-Item .env.example .env
```

Then open `.env` and set the values:

```ini
# --- PostgreSQL ---
DB_URL=postgresql://your_user:your_password@localhost:5432/your_db_name
POSTGRES_DB=your_db_name
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_PORT=5432

# --- Elasticsearch ---
ELASTIC_URL=http://localhost:9200

# --- Groq (AI incident summaries) ---
GROQ_API_KEY=your_groq_api_key
GROQ_MODEL=llama-3.3-70b-versatile     # optional, this is the default
```

| Variable | Required? | Notes |
|---|---|---|
| `DB_URL` | Yes | Must match the three `POSTGRES_*` values below it |
| `POSTGRES_DB` / `USER` / `PASSWORD` | Yes | Used by `docker compose` to create the database |
| `POSTGRES_PORT` | No | Defaults to `5432` |
| `ELASTIC_URL` | Yes | Powers the dashboard's **Log Search** page |
| `GROQ_API_KEY` | No | Needed only for AI incident summaries |
| `GROQ_MODEL` | No | Defaults to `llama-3.3-70b-versatile` |

> `.env` is git-ignored — never commit real credentials.

---

## 5. Install Python dependencies

Create and activate a virtual environment, then install.

**macOS / Linux**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

**Windows (PowerShell)**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

> If PowerShell blocks activation, run once:
> `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`

### Which requirements file?

| File | Contents | Use when |
|---|---|---|
| `requirements.txt` | Everything (pipeline + dashboard + tests) | **Local development — use this** |
| `requirements-base.txt` | Shared runtime deps | Pulled in by the others |
| `requirements-ml.txt` | Lean ML stack, no UI | The `pipeline` container |
| `requirements-dashboard.txt` | ML stack + Streamlit UI | The `dashboard` container |

---

## 6. Start the backing services (Postgres + Elasticsearch)

```bash
docker compose up -d postgres elasticsearch
```

Wait ~30 seconds for Elasticsearch to become healthy, then verify:

```bash
docker compose ps
```

Both should show `healthy`. You can also check directly:

```bash
curl http://localhost:9200          # Elasticsearch should return cluster JSON
```

**Windows (PowerShell)** — `curl` is aliased differently, use:
```powershell
Invoke-RestMethod http://localhost:9200
```

> The pipeline **still runs** if these are down — use `--dry-run` (below). But the
> dashboard's Log Search and incident persistence require them.

---

## 7. Run the ML pipeline

> **Read this first.** The repository ships **no trained model** — the pipeline trains one
> itself. *What it trains on determines how well it works.* Follow steps 7a → 7b in order
> the first time. Section [7c](#7c-why-train-on-a-clean-baseline-first) explains why.

### 7a. Step 1 — Train on a clean baseline (do this first)

An anomaly detector must learn what **normal** looks like before it can recognise
abnormal. So the very first thing you run should be **incident-free logs**:

**macOS / Linux**
```bash
# 1. Generate clean, incident-free logs (~80 files, ~7,000 lines)
python scripts/gen_clean_baseline.py

# 2. Clear any stale model so a genuine cold start happens
rm -f ml/model_store/*.pkl ml/model_store/*.json ml/model_store/retrain_state.json
rm -f data/processed/feature_rolling_store.parquet

# 3. Train the model on the clean logs (--dry-run still trains + saves the model)
python pipeline.py --log-file data/raw/clean_baseline --input-mode synthetic --dry-run
```

**Windows (PowerShell)**
```powershell
python scripts\gen_clean_baseline.py

Remove-Item ml\model_store\*.pkl, ml\model_store\*.json, ml\model_store\retrain_state.json -Force -ErrorAction SilentlyContinue
Remove-Item data\processed\feature_rolling_store.parquet -Force -ErrorAction SilentlyContinue

python pipeline.py --log-file data\raw\clean_baseline --input-mode synthetic --dry-run
```

This trains an IsolationForest on healthy traffic and saves it to `ml/model_store/`.
It also doubles as a smoke test that your whole setup works end to end.

> **Why clear the model store?** If a model already exists, the pipeline loads it instead
> of cold-starting — so you would not actually be training on your clean baseline. On a
> fresh clone the directory is already empty, and these commands are harmless no-ops.

Expected output on this first run — these lines are **normal, not errors**:

```
No saved model found in model_store. Run retrain() first.
No rolling feature store found -- cold start, training on current batch (N rows).
Trained IsolationForest pipeline on N samples, ...
```

### 7b. Step 2 — Run on logs that contain incidents

Now that the model knows what "normal" is, point it at real (or faulty) logs:

```bash
# Dry run first — all stages, but skips the Postgres write
python pipeline.py --log-file data/raw/synthetic_logs_generated --input-mode synthetic --dry-run

# Full run — writes to Postgres + Elasticsearch (requires step 6)
python pipeline.py --log-file data/raw/synthetic_logs_generated --input-mode synthetic
```

Outputs land in `data/processed/` as `.parquet` files. With a full (non-dry) run, results
also go to Postgres and Elasticsearch, and appear in the dashboard.

> Passing **no** `--log-file` makes the pipeline generate synthetic data automatically —
> handy for a quick check, but prefer the explicit two-step flow above.

### 7c. Why train on a clean baseline first?

If your **first** run is a log file full of incidents, the model learns those incidents as
"normal" — a contaminated baseline that weakens every score afterwards. Training on clean
logs first gives it a correct reference for normal behaviour.

**Why isn't a pre-trained model just shipped with the repo?** Deliberately:

- An IsolationForest learns what "normal" means **for one specific environment** — its
  templates, timing, and burst patterns. A model trained on *our* logs would encode the
  wrong notion of "normal" for *your* network.
- The model's sidecar is validated on load: if the `scikit-learn` major version or the
  feature-column list differs, the model is **rejected and retrained anyway**.
- Model binaries don't belong in version control (`ml/model_store/` is git-ignored).

Regenerating a clean baseline takes two commands — strictly better than a frozen binary.

#### Cold-start thresholds

| Setting | Value | Effect |
|---|---|---|
| `MIN_TRAIN_SAMPLES` | 50 | Fewer rows than this → IsolationForest training is skipped; scoring falls back to z-score only |
| `COLD_START_FULL_CONFIDENCE_THRESHOLD` | 500 | Below this many samples the model's contribution is down-weighted in favour of z-score |

So make sure your clean baseline has **at least a few hundred rows** — the bundled
generator produces ~7,000 lines across 80 files, comfortably enough.

#### Model versioning

Each trained model is saved as a timestamped pair:

```
ml/model_store/isolation_forest_v20260703_142530.pkl    # the model
ml/model_store/isolation_forest_v20260703_142530.json   # metadata sidecar
```

The **newest** model is selected automatically on load, and validated against the current
environment before use. You never need to pick a version manually.

To wipe the model and start over, see
[Want a completely fresh ML model](#want-a-completely-fresh-ml-model) in Troubleshooting.

### 7d. Pipeline stages

The pipeline runs these 8 steps in order:

```
parsing → features → anomaly → correlation → scoring → cross_run → evaluate → storage
```

### 7e. Command-line options

| Flag | Values | Purpose |
|---|---|---|
| `--dry-run` | — | Run all steps but skip the Postgres write |
| `--log-file PATH` | file or directory | Input logs (default: `data/raw/sample.log`) |
| `--input-mode MODE` | `auto` \| `syslog` \| `synthetic` | How to interpret the input |
| `--from-step STEP` | any step name above | Resume from a stage, reading earlier parquets from disk |

**Examples**
```bash
# Use your own flat syslog file
python pipeline.py --log-file data/raw/my_switch.log --input-mode syslog

# Re-run only from the scoring stage onward (fast iteration)
python pipeline.py --dry-run --from-step scoring
```

#### About `--input-mode`

| Mode | Meaning |
|---|---|
| `auto` | Directory → synthetic loader; single file → syslog sessionizer |
| `syslog` | Force the flat RFC-3164 syslog parser (use for uploaded log folders) |
| `synthetic` | Force the structured 7-section loader (files with `## SECTION` markers) |

---

## 8. Launch the dashboard

```bash
streamlit run dashboard/app.py
```

Then open <http://localhost:8501>.

### Dashboard pages

| Page | What it shows |
|---|---|
| **Incident Feed** | All detected incidents, filterable by host / severity / time |
| **Incident Detail** | Correlation graph, event timeline, root cause, AI summary, diagnostics |
| **Host Health** | Per-host anomaly rates and incident counts |
| **Log Search** | Full-text search over raw logs (requires Elasticsearch) |
| **Upload & Analyze** | Upload `.log` / `.txt` / archives and run the pipeline from the UI |

---

## 9. Running with your own logs

### Option A — via the dashboard (easiest)

1. Open the **Upload & Analyze** page
2. Drop in one or more `.log` / `.txt` files (or a `.zip` / `.tar.gz` archive)
3. Choose a parsing mode (or leave on **Auto-detect**)
4. Click **Analyze** and watch the live pipeline log
5. Click **View incidents in Incident Feed →**

### Option B — via the command line

```bash
python pipeline.py --log-file /path/to/your/logs.log --input-mode syslog
```

### Supported input formats

| Format | Looks like | Use `--input-mode` |
|---|---|---|
| **Flat syslog** (RFC 3164) | `Mar 01 03:45:00 node-2 worker: ERROR Database connection timeout` | `syslog` |
| **7-section structured** | Contains `## SECTION` headers | `synthetic` |

---

## 10. Verifying it worked

After a successful run you should see these files appear in `data/processed/`:

| File | Produced by |
|---|---|
| `sessionized_logs.parquet` | parsing |
| `features_df.parquet` | features |
| `anomaly_df.parquet` | anomaly |
| `graph_scores_df.parquet` | correlation |
| `scored_logs_df.parquet` | scoring |
| `incident_history.parquet` | cross_run |
| `correlation_graph.json` | correlation (graph export) |

The pipeline also prints a summary of output files and row counts on completion.

**In the dashboard**, a successful run means the **Incident Feed** lists at least one
incident (assuming your logs actually contain faults — a clean log file correctly
produces zero incidents).

### Run the test suite (optional)

```bash
pytest
```

---

## 11. Alternative: run everything in Docker

If you'd rather not install Python locally, the whole stack is containerised.

```bash
# Build and start everything (Postgres, Elasticsearch, dashboard)
docker compose up -d --build

# Run the pipeline once inside its container
docker compose run --rm pipeline

# Dashboard is now at http://localhost:8501
```

To run the pipeline against a different file inside Docker:

```bash
docker compose run --rm pipeline python pipeline.py --log-file data/raw/my_logs.log --input-mode syslog
```

Stop everything:
```bash
docker compose down
```

To also delete the database volume (fresh start):
```bash
docker compose down -v
```

---

## 12. Generating sample data

If you have no real logs, the repo ships generators under `scripts/`:

```bash
# Structured 7-section scenario logs (split-brain, protocol starvation, OOM, ...)
python scripts/gen_vendor_neutral_logs.py

# A purely clean baseline — no injected incidents (useful for training the model on "normal")
python scripts/gen_clean_baseline.py
```

Then point the pipeline at the output directory:

```bash
python pipeline.py --log-file data/raw/synthetic_logs_generated --input-mode synthetic
```

> If you pass no `--log-file` at all, the pipeline generates synthetic data automatically.

---

## 13. Troubleshooting

### `ModuleNotFoundError` when running a script
Run from the **project root**, and use the module flag for submodules:
```bash
python -m storage.db_writer      # not: python storage/db_writer.py
```

### Dashboard shows "Elasticsearch is Unavailable" / Log Search returns nothing
Elasticsearch isn't running, or the logs were indexed while it was down.
```bash
docker compose up -d elasticsearch
```
Then **re-run the pipeline** (without `--dry-run`) so the logs get indexed.

### Log Search finds nothing even though incidents exist
Log Search only covers a **recent time window** (max 7 days). If your log file's
timestamps are older than that, no results will appear — this is expected.

### Dashboard shows no incidents
- Confirm the pipeline finished successfully and wrote `scored_logs_df.parquet`
- Confirm you ran **without** `--dry-run` (dry-run skips the Postgres write)
- Widen the time-range filter in the sidebar to cover your log file's dates
- A genuinely clean log file correctly produces **zero** incidents

### AI Incident Summary panel stays empty
Set `GROQ_API_KEY` in `.env`. Summaries are generated **on demand** when you open an
incident and click *Generate summary* — not during the pipeline run.

### Port already in use
Change the host port in `docker-compose.yml` (e.g. `"5433:5432"`) or set
`POSTGRES_PORT` in `.env`.

### "No saved model found in model_store" on first run
**This is expected, not an error.** No trained model ships with the repo. The pipeline
cold-starts and trains one automatically — see
[7c. Why train on a clean baseline first?](#7c-why-train-on-a-clean-baseline-first).

### Want a completely fresh ML model
Delete the saved models and the rolling training store, then re-run:

**macOS / Linux**
```bash
rm -f ml/model_store/*.pkl ml/model_store/*.json ml/model_store/retrain_state.json
rm -f data/processed/feature_rolling_store.parquet
```

**Windows (PowerShell)**
```powershell
Remove-Item ml\model_store\*.pkl, ml\model_store\*.json, ml\model_store\retrain_state.json -Force -ErrorAction SilentlyContinue
Remove-Item data\processed\feature_rolling_store.parquet -Force -ErrorAction SilentlyContinue
```

### Postgres connection refused
Check the container is healthy and that `DB_URL` matches your `POSTGRES_*` values:
```bash
docker compose ps
docker compose logs postgres
```

---

## 14. Deployment considerations

**Where does this run today?**
It runs **off-box** — as a batch pipeline on a server or laptop that ingests logs
exported from network devices. It is not currently deployed on a switch itself.

**Could it run directly on a switch?**
Not as-is. The current design assumes resources a switch's management CPU typically
does not have:

- **PostgreSQL + Elasticsearch** as backing stores
- A **Python 3.11 + scikit-learn / pandas** runtime
- Memory headroom to build the co-occurrence graph and hold DataFrames in RAM

**A realistic path to on-device use** would be a split architecture:

| Component | Where it runs | Why |
|---|---|---|
| Log shipping / lightweight parsing | On the switch | Cheap; already common (e.g. Fluent Bit) |
| Feature extraction, ML, correlation, scoring | Off-box collector | Needs CPU/RAM and the model store |
| Dashboard / storage | Off-box | Needs Postgres + Elasticsearch |

In other words: keep the switch as a **log producer**, and run the intelligence layer
on a central collector that aggregates many devices — which also gives the cross-device
correlation that a single switch could never see on its own.

---

## Quick reference

```bash
# One-time setup
cp .env.example .env && <edit .env>
python -m venv .venv && source .venv/bin/activate    # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
docker compose up -d postgres elasticsearch

# Run
python pipeline.py --dry-run                         # safe test, no DB write
python pipeline.py                                   # full run, writes to DB
streamlit run dashboard/app.py                       # open http://localhost:8501
```
