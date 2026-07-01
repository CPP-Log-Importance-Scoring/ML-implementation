# 360° Anomaly Detection Model Evaluation Report

**Generated:** 2026-06-26  
**Branch:** `test/pr-63`  
**Evaluated by:** `scripts/run_360_eval.py`

---

## Models Under Test

| ID | Version Timestamp | n_samples | Description |
|----|------------------|-----------|-------------|
| **M-BL** | `v20260626_171339` | 2,558 | **Baseline-only** — Sumukha's pre-trained model (clean data only) |
| **M-BA** | `v20260626_195047` | 2,558 | **Baseline+Anomaly** — auto-retrained by pipeline on the anomalous `pr_test` July 6–10 batch |

---

## Evaluation Datasets

### Anomalous (5 days, July 11–15) — `data/raw/eval_anomalous/`

| File | Date | Scenario | Severity |
|------|------|----------|----------|
| `eval_anomalous_2026-07-11.log` | Jul 11 | OSPF Reconvergence Storm | CRITICAL |
| `eval_anomalous_2026-07-12.log` | Jul 12 | Memory OOM Cascade | CRITICAL |
| `eval_anomalous_2026-07-13.log` | Jul 13 | ASIC ECC Uncorrectable Errors | CRITICAL |
| `eval_anomalous_2026-07-14.log` | Jul 14 | Gradual Memory Leak (drift) | ERROR |
| `eval_anomalous_2026-07-15.log` | Jul 15 | Gradual Thermal Creep (drift) | ERROR |

**Total logs:** 12,309 | **Ground-truth signals:** 63 (0.51%)

### Clean (5 days, July 16–20) — `data/raw/eval_clean/`

| File | Date | Type |
|------|------|------|
| `eval_clean_2026-07-16.log` | Jul 16 | Weekday steady-state |
| `eval_clean_2026-07-17.log` | Jul 17 | Weekday steady-state |
| `eval_clean_2026-07-18.log` | Jul 18 | Weekend quiet |
| `eval_clean_2026-07-19.log` | Jul 19 | Weekend quiet |
| `eval_clean_2026-07-20.log` | Jul 20 | Weekday with maintenance window |

**Total logs:** 19,622 | **Ground-truth signals:** 0 (pure clean)

> [!NOTE]
> Both dataset groups are non-overlapping with training data (June 2026) and pr_test data (July 6–10). Same 5-day batches are reused across both model variants — the controlled variable is the model, not the data.

---

## Results — All 4 Scenarios

| Metric | S1: Anom × BL | S2: Anom × BA | S3: Clean × BL | S4: Clean × BA |
|--------|:---:|:---:|:---:|:---:|
| **Model** | M-BL (v171339) | M-BA (v195047) | M-BL (v171339) | M-BA (v195047) |
| **Total Logs** | 12,309 | 12,309 | 19,622 | 19,622 |
| **Critical rows** | **22** | **17** | **0 ✅** | **0 ✅** |
| **Incidents total** | 139 | 130 | 79 | 65 |
| **Incidents escalated** | **5** | **5** | **0 ✅** | **0 ✅** |
| **Escalated precision** | **1.000** | **1.000** | 0.000 | 0.000 |
| **Signal recall (incident)** | **0.857** | **0.857** | 0.000 | 0.000 |
| **Anomaly recall (log-level)** | 0.159 | 0.143 | 0.000 | 0.000 |
| **ranking_recall@k** | **0.762** | **0.746** | 0.000 | 0.000 |
| **Score separation** | **0.369** | **0.365** | −0.196 | −0.189 |
| **Mean score (signals)** | 0.619 | 0.609 | — | — |
| **Mean score (noise)** | 0.249 | 0.244 | 0.196 | 0.189 |
| **Noise suppression ratio** | 0.525 | 0.547 | **0.726** | **0.752** |
| **Scenario detection rate** | **3/5 (60%)** | **2/5 (40%)** | 0/5 | 0/5 |
| **Anomaly flagged rate** | 34.1% | 31.7% | 20.4% | 18.3% |

---

## Key Findings

### ✅ Finding 1: Both Models Have Zero False Escalations (Specificity = 100%)

On 19,622 clean logs across 5 days:
- **0 critical rows** produced by either model
- **0 incidents escalated** — no false alarms at the alerting level

Neither model creates alert fatigue when given clean traffic. This validates the scoring + incident clustering pipeline.

---

### ✅ Finding 2: 100% Escalated Incident Precision on Anomalous Data

Every incident escalated in S1 and S2 was a true positive:
- S1 (Baseline model): 5/139 → **precision 1.000**
- S2 (B+A model): 5/130 → **precision 1.000**

---

### 🔵 Finding 3: Baseline-Only Model Detects Anomalies Better

| Metric | M-BL | M-BA | Winner |
|--------|:-:|:-:|:-:|
| Anomaly recall | **15.87%** | 14.29% | M-BL +1.6pp |
| ranking_recall@k | **76.19%** | 74.60% | M-BL +1.6pp |
| Scenario detection | **3/5 (60%)** | 2/5 (40%) | M-BL +20pp |
| Score separation | **0.369** | 0.365 | M-BL |
| Critical rows | **22** | 17 | M-BL |

M-BL has a cleaner decision boundary from purely healthy training data, making anomalies more distinctive. M-BA partially learned anomaly patterns as "normal," blurring its contrast threshold.

---

### 🔵 Finding 4: Baseline+Anomaly Model Suppresses More Noise on Clean Data

| Metric | M-BL | M-BA | Winner |
|--------|:-:|:-:|:-:|
| Anomaly flagged rate (clean) | 20.4% | **18.3%** | M-BA −2.1pp |
| Noise suppression ratio | 72.56% | **75.16%** | M-BA +2.6pp |
| Incidents created (clean) | 79 | **65** | M-BA −14 |

M-BA generates fewer spurious incidents on clean data — a useful property. However, since both models produce zero escalations on clean data, this difference is operationally moot at the alerting level.

---

### ⚠️ Finding 5: Gradual Drift Is Harder to Detect for Both Models

- M-BL detected **3/5** scenarios (60%) — likely missed 1 drift scenario
- M-BA detected **2/5** scenarios (40%) — missed both drift scenarios

Drift (memory leak, thermal creep) manifests as slow metric slope changes over hours. The IsolationForest sees `metric_slope_short/long` features but individual gradual-drift log-level scores are too low to cluster into escalated incidents. Burst-style incidents (OOM, OSPF storm, ECC) are reliably detected.

---

### ℹ️ Methodology Note

The retrain-freeze mechanism (setting `unprocessed_logs_count = K-1`) did not fully prevent `maybe_retrain()` from firing on the large eval batches (~12K logs, K=1000). Since retraining happens **after inference** in the pipeline, all anomaly scores were computed by the correct frozen model — results are valid. Spurious retrained models were removed post-evaluation.

---

## Score Distribution

```
                        S1 (Anom × BL)    S2 (Anom × BA)
Signal mean score:           0.619              0.609
Noise mean score:            0.249              0.244
Separation:                  0.370 ▲            0.365 ▲

                        S3 (Clean × BL)   S4 (Clean × BA)
Mean noise score:            0.196              0.189
(no signals present — lower is better)
```

Both models maintain ~0.37 signal-noise separation on anomalous data.

---

## Recommendations

| Priority | Recommendation | Rationale |
|----------|---------------|-----------|
| 🔴 High | **Use M-BL as production model** | Better recall, scenario detection, score separation — with identical zero-FP escalation record |
| 🟡 Med | **Retrain M-BA on balanced clean+anomaly data** | Currently trained on one anomalous batch only; a balanced retrain could close the gap |
| 🟡 Med | **Improve drift detection** | Add a separate drift-detection module or heavier feature engineering for monotonic slope trends |
| 🟢 Low | **Add `--no-retrain` flag to pipeline CLI** | Ensures model stays frozen during evaluation without hacking retrain_state |

---

## Appendix — Raw Oracle Metrics

````carousel
**Scenario 1 — Anomalies × Baseline Model (M-BL)**

```
total_logs:                 12,309
truth_signal_count:         63
anomaly_flagged_count:      4,192 (34.1%)
anomaly_tp / fp / fn:       10 / 4,182 / 53
anomaly_recall:             0.1587
ranking_recall_at_k:        0.7619
score_separation:           0.3692
n_incidents:                139
n_incidents_escalated:      5
escalated_precision:        1.0000
incident_signal_recall:     0.8571
scenario_detection_rate:    3/5 (60%)
```
<!-- slide -->
**Scenario 2 — Anomalies × Baseline+Anomaly Model (M-BA)**

```
total_logs:                 12,309
truth_signal_count:         63
anomaly_flagged_count:      3,902 (31.7%)
anomaly_tp / fp / fn:       9 / 3,893 / 54
anomaly_recall:             0.1429
ranking_recall_at_k:        0.7460
score_separation:           0.3649
n_incidents:                130
n_incidents_escalated:      5
escalated_precision:        1.0000
incident_signal_recall:     0.8571
scenario_detection_rate:    2/5 (40%)
```
<!-- slide -->
**Scenario 3 — Clean × Baseline Model (M-BL)**

```
total_logs:                 19,622
truth_signal_count:         0
anomaly_flagged_count:      4,009 (20.4%)
anomaly_tp / fp / fn:       0 / 4,009 / 0
n_incidents:                79
n_incidents_escalated:      0   ← zero false escalations
noise_suppression_ratio:    0.7256
```
<!-- slide -->
**Scenario 4 — Clean × Baseline+Anomaly Model (M-BA)**

```
total_logs:                 19,622
truth_signal_count:         0
anomaly_flagged_count:      3,597 (18.3%)
anomaly_tp / fp / fn:       0 / 3,597 / 0
n_incidents:                65
n_incidents_escalated:      0   ← zero false escalations
noise_suppression_ratio:    0.7516
```
````
