"""
=====================================================================================================
SCRIPT   : Run a whole month without Airflow
-----------------------------------------------------------------------------------------------------
Runs every job of one return period in the same order as the three Airflow DAGs.
Use it to re-run or back-fill a month, or to test changes on a local copy of the data.

  GSTR-1 track, per batch (DAG 1):  1 validation -> 2 error fixing -> 1 re-validation -> 3 upload file
  GSTR-1 track, per month (DAG 2):  4 combine batches -> 5 EINV reco + 6 EWB reco
  PR / 2B track (DAG 3):            7 PR validation + 8 2B consolidation -> 9 2B vs PR reco

It does NOT write batch_status/*.done markers - those belong to the Airflow run of DAG 1.

HOW TO RUN (from the repository root)
  python scripts/run_month.py --period 202506
  python scripts/run_month.py --period 202506 --local-dir data      (local copy of the bucket)
  python scripts/run_month.py --period 202506 --only gstr1           (only DAG 1 + DAG 2 jobs)
  python scripts/run_month.py --period 202506 --only pr              (only DAG 3 jobs)
=====================================================================================================
"""
import argparse
import json
import os
import sys
import time

import boto3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from activities.combine_batches.job import run as combine_batches                       # noqa: E402
from activities.einv_vs_sales_reco.job import run as einv_vs_sales_reco                 # noqa: E402
from activities.ewb_vs_sales_reco.job import run as ewb_vs_sales_reco                   # noqa: E402
from activities.gstr1_error_fixing.job import run as gstr1_error_fixing                 # noqa: E402
from activities.gstr1_upload_file_creation.job import run as gstr1_upload_file_creation # noqa: E402
from activities.gstr1_validation.job import run as gstr1_validation                     # noqa: E402
from activities.gstr2b_consolidation.job import run as gstr2b_consolidation             # noqa: E402
from activities.gstr2b_vs_pr_reco.job import run as gstr2b_vs_pr_reco                   # noqa: E402
from activities.pr_validation.job import run as pr_validation                           # noqa: E402


def read_manifest(period: str, local_dir: str = None) -> list:
    """Batch list for the month, from raw/<period>/gstr1/manifest.json"""
    key = f"raw/{period}/gstr1/manifest.json"
    if local_dir:
        with open(os.path.join(local_dir, key)) as f:
            manifest = json.load(f)
    else:
        s3 = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-south-1"))
        body = s3.get_object(Bucket=os.environ.get("S3_BUCKET", "swiggy-idt-poc-bucket"), Key=key)["Body"].read()
        manifest = json.loads(body)
    return manifest.get("batches") or [f"B{n}" for n in range(1, manifest["total_batches"] + 1)]


def main():
    parser = argparse.ArgumentParser(description="Run every job of one month in DAG order")
    parser.add_argument("--period", required=True, help="return period YYYYMM, e.g. 202506")
    parser.add_argument("--local-dir", help="use this local folder instead of S3")
    parser.add_argument("--only", choices=["gstr1", "pr"], help="run only one track")
    args = parser.parse_args()
    period, local = args.period, args.local_dir
    started = time.time()

    if args.only in (None, "gstr1"):
        batches = read_manifest(period, local)
        print(f"\n===== {period} | GSTR-1 track | batches {batches} =====")
        for batch in batches:
            gstr1_validation(period, batch, "raw", local)
            gstr1_error_fixing(period, batch, local)
            gstr1_validation(period, batch, "corrected", local)
            gstr1_upload_file_creation(period, batch, local)
        combine_batches(period, local)
        einv_vs_sales_reco(period, local)
        ewb_vs_sales_reco(period, local)

    if args.only in (None, "pr"):
        print(f"\n===== {period} | PR / GSTR-2B track =====")
        pr_validation(period, local)
        gstr2b_consolidation(period, local)
        gstr2b_vs_pr_reco(period, local)

    print(f"\n===== {period} done in {time.time() - started:.0f}s =====")


if __name__ == "__main__":
    main()
