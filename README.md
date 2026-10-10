# Swiggy GST Automation Demo — DuckDB + S3 + Airflow

Proof of concept for replacing Alteryx + VDI GST workflows with Python, DuckDB, AWS S3 and Apache Airflow.
All 9 monthly Swiggy GST activities run over **3 months of data** (202504, 202505, 202506) kept permanently in S3.

```
raw/<period>/ (client Excel) → DuckDB → intermediate/<period>/ (parquet) → final/<period>/ (Excel for client)
                                                    └── one DuckDB query across every month (history)
```

## Prerequisites
- Python 3.11+: `pip install -r requirements.txt`
- AWS CLI configured (`aws configure`, region `ap-south-1`). Airflow reuses `~/.aws` read-only.
- Docker Desktop running
- S3 bucket `swiggy-idt-poc-bucket` (override with env var `S3_BUCKET`)

## Run it

```powershell
# 1. Generate 3 months of inputs (sample_data\raw\...) and upload them to S3 raw/
python pipeline\create_sample_data.py              # or --periods 202506, or --no-upload

# 2. Validate one batch locally (no S3, no Airflow) -> output\202506\gstr1_validated_B1.xlsx
python activities\gstr1_validation\src\test_local.py --period 202506 --batch B1

# 3. All 9 activities for every month from S3, then the cross-month history query (~30 s)
python pipeline\run_demo.py                        # or --periods 202506

# 4. Airflow (first start takes ~2 min while it installs duckdb)
docker compose up -d                               # http://localhost:8080  admin / admin
# port 8080 busy?  $env:AIRFLOW_PORT = "8081"; docker compose up -d
```

In Airflow:
1. Trigger `swiggy_gstr1_batch_dag` once per batch with config `{"period": "202505", "batch_id": "B1"}`, then B2, B3 and B4.
   The manifest for 202505 says 4 batches. After B4, DAG 1 starts `swiggy_gstr1_combine_reco_dag` automatically.
2. Trigger `swiggy_2b_pr_dag` with `{"period": "202505"}`.

To re-run the Airflow demo for a month, clear its batch markers first. `run_demo.py` also writes them.
```powershell
aws s3 rm s3://swiggy-idt-poc-bucket/batch_status/202505/ --recursive
```

## Months in the sample data

| Period | Batches (rows) | Notes |
|---|---|---|
| 202504 | B1 (18), B2 (12) | |
| 202505 | B1 (25), B2 (20), B3 (15), B4 (10) | biggest month |
| 202506 | B1 (20), B2 (15), B3 (10) | |

The data is reproducible: each month uses a fixed random seed. When run, the generator prints what it planted:
- GSTR1 errors, and how many of them the client corrects
- EINV/EWB/2B rows that are dropped, have changed amounts or have typos in the invoice number
- records that exist only in EINV, EWB or 2B

The reco outputs should show exactly these numbers.

## Activities

| # | Activity | Module | Input → Output |
|---|---|---|---|
| 1 | GSTR1 validation | `activities/gstr1_validation` | `gstr1_Bx.xlsx` → `Bx_validated.parquet`, `Bx_errors_for_client.xlsx` |
| 2 | GSTR1 error fixing | `activities/gstr1_error_fixing` | + `corrections_Bx.xlsx` → `Bx_fixed.parquet` (revalidated) |
| 3 | Upload file creation | `activities/gstr1_upload_file_creation` | → `Bx_upload_file.xlsx` (B2B, B2CS, CDNR, CDNUR, EXP) |
| 4 | Combine batches | `activities/combine_batches` | all `Bx_fixed` → `combined_sales.parquet` |
| 5 | EINV vs sales reco | `activities/einv_vs_sales_reco` | `einv_bolton.xlsx` → `einv_vs_sales_reco.xlsx` |
| 6 | EWB vs sales reco | `activities/ewb_vs_sales_reco` | `ewb_bolton.xlsx` → `ewb_vs_sales_reco.xlsx` |
| 7 | PR validation | `activities/pr_validation` | `purchase_register.xlsx` → `pr_validated.parquet`, `pr_errors_for_client.xlsx` |
| 8 | 2B consolidation | `activities/gstr2b_consolidation` | `2b_<GSTIN>.xlsx` × N (B2B + CDNR sheets) → `2b_consolidated.parquet` |
| 9 | 2B vs PR reco | `activities/gstr2b_vs_pr_reco` | → `2b_vs_pr_reco.xlsx` |

Each activity has `src/transformation.py` with a pure `run(...) -> dict` (DataFrames in, DataFrames out, no S3).
[pipeline/steps.py](pipeline/steps.py) wraps each one with S3 reads and writes. Both `run_demo.py` and the DAGs call these same steps.

**Reconciliation ladder** ([activities/common/ladder_match.py](activities/common/ladder_match.py), DuckDB). Each level matches only the rows left unmatched by the levels before it:

| Level | Keys | Catches |
|---|---|---|
| L1 Exact | GSTIN + normalised invoice no + amount ±1 | `SWG/202506/B1001` = `SWG-202506-B1001` |
| L2 GSTIN+Invoice | GSTIN + normalised invoice no | amount differences |
| L3 GSTIN+Amount | GSTIN + amount ±1 | typos in the invoice number |
| Only in X | — | missing on the other side |

## DAGs

| DAG | Trigger | Tasks |
|---|---|---|
| `swiggy_gstr1_batch_dag` | per batch (`period`, `batch_id`) | read_manifest → gstr1_validation → gstr1_error_fixing → gstr1_upload_file_creation → mark_batch_complete → check_all_batches_done → trigger_combine_dag \| wait_for_more_batches |
| `swiggy_gstr1_combine_reco_dag` | DAG 1, when the `.done` count equals the manifest | combine_batches → [einv_vs_sales_reco ∥ ewb_vs_sales_reco] → generate_summary |
| `swiggy_2b_pr_dag` | manual (`period`) | [pr_validation ∥ gstr2b_consolidation] → gstr2b_vs_pr_reco → generate_summary |

## S3 layout (per period)

```
raw/<p>/gstr1/manifest.json                    {"total_batches": N}
raw/<p>/gstr1/Bx/gstr1_Bx.xlsx, corrections_Bx.xlsx, trigger_Bx.json
raw/<p>/einv_bolton/einv_bolton.xlsx
raw/<p>/ewb_bolton/ewb_bolton.xlsx
raw/<p>/pr/purchase_register.xlsx
raw/<p>/2b_bolton/2b_<GSTIN>.xlsx

intermediate/<p>/gstr1/Bx_validated.parquet, Bx_fixed.parquet
intermediate/<p>/combined_sales.parquet, einv.parquet, ewb.parquet, pr_validated.parquet, 2b_consolidated.parquet
intermediate/<p>/reco/einv_vs_sales.parquet, ewb_vs_sales.parquet, 2b_vs_pr.parquet

batch_status/<p>/Bx.done

final/<p>/gstr1/Bx_upload_file.xlsx, Bx_errors_for_client.xlsx
final/<p>/einv_vs_sales_reco.xlsx, ewb_vs_sales_reco.xlsx, 2b_vs_pr_reco.xlsx, pr_errors_for_client.xlsx
final/<p>/summaries/gstr1_summary.xlsx, pr_2b_summary.xlsx
```

Every parquet file has a `period` column, so the history can be queried across months: in `steps.cross_month_analysis()` today, and later in Athena.

## Repo layout
```
activities/<activity>/src/transformation.py   9 activities + common/ (GST utils, ladder match, sales reco)
pipeline/       create_sample_data.py, steps.py (S3 step per task), run_demo.py, write_to_s3.py
dags/           3 Airflow DAGs
sample_data/raw/<period>/...   local mirror of S3 raw/ (committed)
docker-compose.yml             Airflow 2.9 standalone
```

Not in scope yet: email notifications, Athena/Glue setup, waiting for real client corrections between steps.
