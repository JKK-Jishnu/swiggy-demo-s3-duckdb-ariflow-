# Swiggy GST Automation

The monthly GST compliance work for Swiggy runs as Python + DuckDB jobs, orchestrated by Apache Airflow.
All data is kept in Amazon S3, and every month of history can be queried in Amazon Athena.

| Track | What it produces |
|---|---|
| **GSTR-1** (sales) | Validated sales register, client corrections applied, GSTN upload file, EINV and EWB reconciliations |
| **PR / GSTR-2B** (purchases) | Validated purchase register, consolidated GSTR-2B, 2B vs PR reconciliation with ITC at risk |

History in Athena starts with return period **202504** (April 2025).

---

## 1. How a month runs

```
Client uploads (S3 raw/<period>/)
│
├── GSTR-1 track
│     DAG 1  swiggy_gstr1_batch_dag          once per batch (B1, B2, ... as listed in manifest.json)
│            1 GSTR-1 validation ─► 2 error fixing ─► 1 re-validation ─► 3 upload file ─► mark batch done
│            when every batch in the manifest is done ─► starts DAG 2
│     DAG 2  swiggy_gstr1_combine_reco_dag   once per month
│            4 combine batches ─► 5 EINV vs sales reco
│                              └► 6 EWB  vs sales reco
│
└── PR / GSTR-2B track
      DAG 3  swiggy_2b_pr_dag                once per month
             7 PR validation    ─┐
             8 2B consolidation ─┴► 9 2B vs PR reco
```

Outputs per month:
- `intermediate/<table>/period=<period>/`: parquet files with every line, used by the next job and by Athena.
- `final/<period>/`: Excel files for the client and for the GSTN portal.

---

## 2. The jobs

Each job is **one self-contained file**, `activities/<job>/job.py`. It holds everything the job needs:
- column mappings and rules
- reading and writing S3
- the transformation steps

Jobs share no code, so changing one job can never break another.

| # | Job (folder) | Runs | Input → Output | Alteryx equivalent |
|---|---|---|---|---|
| 1 | `gstr1_validation` | per batch, twice | sales register → `gstr1_validated` / `gstr1_final` + error Excel | Input → Select → Formula per rule → Filter → Output |
| 2 | `gstr1_error_fixing` | per batch | register + client corrections → corrected register + change log | Join on Record ID → Formula → Output |
| 3 | `gstr1_upload_file_creation` | per batch | final lines → GSTN upload Excel (b2b, b2cl, b2cs, cdnr, cdnur, exp, hsn, docs) | Filter → Summarize → Select → Output |
| 4 | `combine_batches` | per month | all batches → `sales_combined` (duplicate flags) | Directory → Union → Formula |
| 5 | `einv_vs_sales_reco` | per month | EINV (IRN) vs sales → reco Excel | Join ladder → Union J/L/R → Summarize |
| 6 | `ewb_vs_sales_reco` | per month | EWB vs sales → reco Excel | Join ladder → Union J/L/R → Summarize |
| 7 | `pr_validation` | per month | purchase register → `pr_validated` + error Excel | Input → Select → Formula per rule → Filter |
| 8 | `gstr2b_consolidation` | per month | 2B workbooks (B2B + CDNR sheets) → `gstr2b_consolidated` | Dynamic Input → Select → Union |
| 9 | `gstr2b_vs_pr_reco` | per month | 2B vs PR → reco Excel with ITC at risk | Join ladder → Union J/L/R → Summarize |

The header of every `job.py` lists:
- what the job does and its Alteryx equivalent
- exact input and output paths
- every validation rule (R01–R18 for GSTR-1, P01–P17 for purchases)
- how to run it

### Reconciliation ladder (jobs 5, 6, 9)

Each level only looks at rows that no earlier level matched. A row is matched at most once.

| Level | Match on | Meaning |
|---|---|---|
| L1 Exact | GSTIN + invoice number + taxable value (±Rs.1) | fully matched |
| L2 Amount mismatch | GSTIN + invoice number | same document, different amount |
| L3 Invoice no mismatch | GSTIN + taxable value (±Rs.1) | same amount, invoice number typed differently |
| Only in … | — | present on one side only |

Invoice numbers are compared without `/`, `-` and spaces and ignoring case, so `SWG/2506/00012` = `SWG-2506-00012`.
2B vs PR matching also requires the same Swiggy GSTIN.

---

## 3. Record IDs: tracing a line through every file

Every input line carries a **Record ID** from the client file onwards. Jobs never change it, so the same ID is the lookup key in every later file and in Athena.

| Source | Format | Example |
|---|---|---|
| Sales register (GSTR-1) | `sales-<period>-<type>-<n>` | `sales-202506-b2b-1`, `sales-202506-b2c-1` |
| Purchase register | `pr-<period>-<n>` | `pr-202506-1` |
| GSTR-2B | `2b-<period>-<n>` | `2b-202506-10` |
| E-invoice (IRN) | `einv-<period>-<n>` | `einv-202506-3` |
| E-way bill | `ewb-<period>-<n>` | `ewb-202506-7` |

Where lines are **summed** (upload file sections b2cs, hsn and docs, and every reconciliation Summary sheet), the IDs of all lines behind the figure are joined with ` | ` in the Record ID column.

How to trace a line:

| You want | Look at |
|---|---|
| A sales line's whole journey (first validation → correction → final status → upload section → EINV / EWB match) | `SELECT * FROM swiggy_gst.v_sales_record_trace WHERE record_id = 'sales-202506-b2b-1'` |
| The upload file rows built from a sales line | `athena/queries/02_find_record_in_upload_file.sql` |
| A purchase line and its 2B match | `SELECT * FROM swiggy_gst.v_purchase_record_trace WHERE record_id = 'pr-202506-1'` |
| The same, in Excel | filter the `Record ID(s)` / `*_record_id` columns of any output file |

Validation outputs also keep `source_row_number`, the row of the line in the client's Excel file.

---

## 4. S3 layout

Bucket `swiggy-idt-poc-bucket` (ap-south-1):

```
raw/<period>/                                    client uploads - never modified
    gstr1/manifest.json                          {"total_batches": 3, "batches": ["B1","B2","B3"]}
    gstr1/<batch>/gstr1_<batch>.xlsx             sales register        (sheet "Sales Register")
    gstr1/<batch>/corrections_<batch>.xlsx       client corrections    (sheet "Corrections", keyed by Record ID)
    gstr1/<batch>/trigger_<batch>.json           upload-complete signal for DAG 1
    einv_bolton/einv_bolton.xlsx                 IRN data              (sheet "EINV")
    ewb_bolton/ewb_bolton.xlsx                   e-way bills           (sheet "EWB")
    pr/purchase_register.xlsx                    purchase register     (sheet "Purchase Register")
    2b_bolton/2b_<GSTIN>.xlsx                    GSTR-2B per Swiggy GSTIN (sheets "B2B", "CDNR")

intermediate/<table>/period=<period>/*.parquet   one folder per Athena table (see section 6)
intermediate/gstr1_corrected/ ...                corrected register (internal hand-over 2 -> 1)
intermediate/gstr1_final/ ...                    re-validated lines per batch (internal hand-over 1 -> 3, 4)

batch_status/<period>/<batch>.done               written by DAG 1 when a batch is finished

final/<period>/gstr1/<batch>_errors_for_client.xlsx          first validation errors -> client
final/<period>/gstr1/<batch>_corrections_log.xlsx            every field the client corrected (old / new)
final/<period>/gstr1/<batch>_errors_after_correction.xlsx    still wrong after corrections
final/<period>/gstr1/<batch>_gstr1_upload.xlsx               GSTN offline-tool upload file
final/<period>/einv_vs_sales_reco.xlsx                       Summary + Reco Details
final/<period>/ewb_vs_sales_reco.xlsx
final/<period>/pr_errors_for_client.xlsx
final/<period>/gstr2b_vs_pr_reco.xlsx

athena-results/                                  Athena query results
```

---

## 5. Running the pipelines

### Setup (once)
```powershell
pip install -r requirements.txt
aws configure                           # region ap-south-1
docker compose up -d                    # Airflow at http://localhost:8080 (admin / admin)
# port 8080 busy?  $env:AIRFLOW_PORT = "8081"; docker compose up -d
python athena/athena.py setup           # Glue database, tables and views for Athena
```

### Monthly run (Airflow)
1. For each batch the client uploads, trigger **`swiggy_gstr1_batch_dag`** with config `{"period": "202506", "batch_id": "B1"}`.
   DAG 2 starts by itself after the last batch in the manifest.
2. When the purchase register and the GSTR-2B files are in `raw/<period>/`, trigger **`swiggy_2b_pr_dag`** with `{"period": "202506"}`.

### Re-run a month (DAG 1 again)
Delete the month's batch markers first. Otherwise DAG 2 starts after the first batch:
```powershell
aws s3 rm s3://swiggy-idt-poc-bucket/batch_status/202506/ --recursive
```

### Run outside Airflow
```powershell
python -m activities.gstr1_validation.job --period 202506 --batch B1     # one job
python scripts/run_month.py --period 202506                              # every job of a month, DAG order
```

### Test on a local copy (no changes in S3)
Every job accepts `--local-dir <folder>`. It then reads and writes the same paths inside that folder instead of the bucket:
```powershell
aws s3 sync s3://swiggy-idt-poc-bucket/raw/202506 data/raw/202506
python scripts/run_month.py --period 202506 --local-dir data
```
(`data/` is git-ignored.)

---

## 6. History in Athena

`python athena/athena.py setup` creates:
- workgroup **`swiggy-gst`**
- Glue database **`swiggy_gst`**
- one table per job output, with column descriptions visible in the Glue console

Every table has a partition column `period` (YYYYMM). New months show up automatically through partition projection, so no crawler or `MSCK REPAIR` is needed.

| Table | One row per |
|---|---|
| `gstr1_validated` | sales line, first validation (as the client uploaded it) |
| `sales_combined` | sales line, after corrections and re-validation, all batches |
| `gstr1_upload_file` | upload file row (`section` = b2b, b2cl, b2cs, cdnr, cdnur, exp, hsn, docs) |
| `einv_records` / `ewb_records` | IRN / e-way bill |
| `einv_vs_sales_reco` / `ewb_vs_sales_reco` | matched pair or unmatched line |
| `pr_validated` | purchase line with validation result |
| `gstr2b_consolidated` | GSTR-2B line |
| `gstr2b_vs_pr_reco` | matched pair or unmatched line, with `itc_at_risk` |

| View | Shows |
|---|---|
| `v_gstr1_monthly_summary` | per month: lines, errors found, corrected, still open, value uploaded |
| `v_reco_monthly_summary` | per month × reconciliation × match level: counts, differences, ITC at risk |
| `v_itc_at_risk` | every purchase line with ITC not supported by GSTR-2B |
| `v_sales_record_trace` | full journey of each sales Record ID |
| `v_purchase_record_trace` | each purchase Record ID with its 2B match |

Ready-made queries live in [athena/queries/](athena/queries/). Run them in the Athena console (workgroup `swiggy-gst`) or from the command line:
```powershell
python athena/athena.py query athena/queries/03_gstr1_monthly_trend.sql
python athena/athena.py query "SELECT * FROM swiggy_gst.v_itc_at_risk"
```

---

## 7. Notes for Alteryx developers

Every `job.py` follows the same layout:

```
header block      what / inputs / outputs / rules / how to run
CONFIGURATION     column mappings (= Select tool), valid codes, tolerances
STORAGE           read/write S3 or a local folder (= Input Data / Output Data tools)
run()             STEP 1, STEP 2, ... - each step is one "tool", with a comment saying which
```

| Alteryx tool | In the code |
|---|---|
| Input Data | `store.read_excel_as_text(...)`. Every cell is read as text; numbers and dates are converted on purpose in the cleansing step |
| Select | `COLUMN_MAP` (rename) and the final `SELECT` (column order and types) |
| Data Cleansing | `trim`, `upper`, `NULLIF(x, '')`, `TRY_CAST(... AS DOUBLE)`, `try_strptime(...)` |
| Formula | a SQL column: `CASE WHEN ... THEN ... END AS new_column` |
| Filter | `WHERE ...` |
| Join (J / L / R) | `JOIN`, plus `NOT IN (...)` for the unjoined left / right rows |
| Summarize | `GROUP BY` with `sum()`, `count()`, `string_agg()` |
| Union | `UNION ALL` / `pa.concat_tables` / `pd.concat` |
| Output Data | `store.write_parquet(...)` (Athena) and `store.write_excel(...)` (client) |

Each SQL step creates a named table (`CREATE TABLE cleaned AS ...`, `CREATE TABLE checked AS ...`), like the output anchor of a tool. To check an intermediate result while developing, `print(con.execute("SELECT * FROM checked LIMIT 20").df())`.

**Adding a validation rule** (example in `gstr1_validation/job.py`):
1. Add one line to the rules list in the header (`R19 ...`).
2. Add one `COALESCE(<condition>, false) AS r19_ok` column in STEP 5.
3. Add `+ CAST(NOT r19_ok AS INTEGER)` to `error_count` and a `CASE WHEN NOT r19_ok THEN 'R19 ...' END` line to the remark in STEP 6.

---

## 8. Repository layout

```
activities/<job>/job.py      the 9 jobs, one self-contained file each
dags/                        the 3 Airflow DAGs (they only call job.run())
scripts/run_month.py         run every job of a month without Airflow
athena/athena.py             create Athena / Glue objects, run queries
athena/tables/*.sql          one CREATE TABLE per job output
athena/views/*.sql           summary and trace views
athena/queries/*.sql         ready-made history queries
docker-compose.yml           Airflow 2.9 (standalone)
requirements.txt
```
