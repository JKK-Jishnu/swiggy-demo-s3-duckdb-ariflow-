# Swiggy GST Automation Demo — DuckDB + S3 + Airflow

Proof of concept for replacing Alteryx + VDI GST workflows with Python, DuckDB, AWS S3 and Apache Airflow.
It covers Swiggy GSTR1 validation across dynamic batches (B1..Bx). When all batches are done, it combines them and builds a summary.

```
raw/ (Excel from client) → DuckDB validation → intermediate/ (parquet) → combine → final/ (Excel summary)
```

## Prerequisites
- Python 3.11+: `pip install -r requirements.txt`
- AWS CLI configured (`aws configure`, region `ap-south-1`). Airflow reuses `~/.aws` read-only.
- Docker Desktop running
- S3 bucket `swiggy-idt-poc-bucket` (override with env var `S3_BUCKET`)

## Run it

```powershell
# 1. Generate 3 batches (20/15/10 rows, ~25% injected errors) and upload to S3
python pipeline\create_sample_data.py

# 2. Validate one batch locally (no S3, no Airflow) -> data\output\B1\gstr1_validated_B1.xlsx
python activities\gstr1_validation\src\test_local.py --batch B1

# 3. Full pipeline from S3: validate all batches, write parquet, combine, DuckDB summary
python pipeline\run_demo.py

# 4. Airflow (first start takes ~2 min while it installs duckdb)
docker compose up -d                       # http://localhost:8080  admin / admin
# port 8080 busy?  $env:AIRFLOW_PORT = "8081"; docker compose up -d
```

In the Airflow UI, trigger `swiggy_gstr1_batch_dag` with config `{"period": "202506", "batch_id": "B1"}`. Repeat for B2 and B3.
After B3 the `.done` count matches the manifest, so `swiggy_gstr1_combine_reco_dag` starts automatically.

To re-run the Airflow demo from scratch, clear the batch markers first. `run_demo.py` also writes them.
```powershell
aws s3 rm s3://swiggy-idt-poc-bucket/batch_status/202506/ --recursive
```

## DAGs

| DAG | Trigger | Tasks |
|---|---|---|
| `swiggy_gstr1_batch_dag` | per batch (`period`, `batch_id`) | read_manifest → gstr1_validation → mark_batch_complete → check_all_batches_done → trigger_combine_dag \| wait_for_more_batches |
| `swiggy_gstr1_combine_reco_dag` | DAG 1 when all batches done | combine_batches → generate_summary |

## S3 layout

```
raw/202506/gstr1/manifest.json                 {"total_batches": 3, ...}
raw/202506/gstr1/B1/gstr1_B1.xlsx              + trigger_B1.json
intermediate/202506/gstr1/B1_validated.parquet
intermediate/202506/combined_sales.parquet
batch_status/202506/B1.done
final/202506/summaries/gstr1_summary.xlsx      Summary + Errors sheets
```

## Validation rules (`activities/gstr1_validation/src/transformation.py`)
Each rule runs as a DuckDB SQL check. GSTIN format is checked by a Python UDF registered in DuckDB.
- Receiver GSTIN matches the 15-character GSTIN format
- Invoice type is one of B2B, B2C, CDNR, CDNUR, EXP
- Taxable value > 0
- Invoice number and GSTIN present
- Invoice date parses and is not in the future

Every row gets `validation_status` (Success/Error) and a `validation_remark` that lists every failed check.

## Repo layout
```
pipeline/            create_sample_data.py, run_demo.py, write_to_s3.py (shared S3 helpers)
activities/          gstr1_validation/ (built), combine_batches/, einv_vs_sales_reco/ (placeholders)
dags/                Airflow DAGs
docker-compose.yml   Airflow 2.9 standalone
```

Not in scope yet: error fixing, portal upload file, EINV/EWB reco, PR/2B track, email, Athena.
