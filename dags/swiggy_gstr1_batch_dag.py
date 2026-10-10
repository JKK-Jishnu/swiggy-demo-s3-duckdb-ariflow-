"""
=====================================================================================================
DAG 1    : swiggy_gstr1_batch_dag                    GSTR-1 track, one run per batch
-----------------------------------------------------------------------------------------------------
TRIGGER  : when the client uploads raw/<period>/gstr1/<batch>/trigger_<batch>.json
           (or manually in the Airflow UI with config {"period": "202506", "batch_id": "B1"})

FLOW
  read_manifest                  how many batches does this month have?
  gstr1_validation               Activity 1 on the register as uploaded
  gstr1_error_fixing             Activity 2 - apply the client's corrections
  gstr1_revalidation             Activity 1 again, on the corrected register
  gstr1_upload_file_creation     Activity 3 - GSTN offline-tool upload file
  mark_batch_complete            write batch_status/<period>/<batch>.done
  check_all_batches_done         count .done files vs the manifest
     -> trigger_combine_dag      all batches done: start DAG 2 for this period
     -> wait_for_more_batches    otherwise stop here; the next batch will check again

Each task only calls the job's run() function - all business logic lives in activities/<job>/job.py
=====================================================================================================
"""
import json
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, "/opt/airflow")

BUCKET = os.environ.get("S3_BUCKET", "swiggy-idt-poc-bucket")
REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")


def _period_and_batch(context):
    return context["params"]["period"], context["params"]["batch_id"]


def _s3():
    import boto3
    return boto3.client("s3", region_name=REGION)


def _expected_batches(period: str) -> list:
    """Batches of the month from raw/<period>/gstr1/manifest.json, e.g. ["B1", "B2", "B3"]"""
    body = _s3().get_object(Bucket=BUCKET, Key=f"raw/{period}/gstr1/manifest.json")["Body"].read()
    manifest = json.loads(body)
    if "batches" in manifest:
        return manifest["batches"]
    return ["B" + str(number) for number in range(1, manifest["total_batches"] + 1)]


# ----------------------------------------------------------------------------------------- tasks
def read_manifest(**context):
    period, batch = _period_and_batch(context)
    batches = _expected_batches(period)
    if batch not in batches:
        raise ValueError(f"Batch {batch} is not in the {period} manifest {batches}")
    print(f"Manifest {period}: {len(batches)} batches {batches}")
    return len(batches)


def gstr1_validation(**context):
    from activities.gstr1_validation.job import run
    period, batch = _period_and_batch(context)
    return run(period, batch, stage="raw")


def gstr1_error_fixing(**context):
    from activities.gstr1_error_fixing.job import run
    period, batch = _period_and_batch(context)
    return run(period, batch)


def gstr1_revalidation(**context):
    from activities.gstr1_validation.job import run
    period, batch = _period_and_batch(context)
    return run(period, batch, stage="corrected")


def gstr1_upload_file_creation(**context):
    from activities.gstr1_upload_file_creation.job import run
    period, batch = _period_and_batch(context)
    return run(period, batch)


def mark_batch_complete(**context):
    period, batch = _period_and_batch(context)
    _s3().put_object(Bucket=BUCKET, Key=f"batch_status/{period}/{batch}.done", Body=b"done")
    print(f"Marked batch_status/{period}/{batch}.done")


def check_all_batches_done(**context):
    period, _ = _period_and_batch(context)
    expected = _expected_batches(period)

    # batch_status/202506/B1.done  ->  "B1"
    listing = _s3().list_objects_v2(Bucket=BUCKET, Prefix=f"batch_status/{period}/")
    done = []
    for obj in listing.get("Contents", []):
        file_name = obj["Key"].split("/")[-1]
        if file_name.endswith(".done"):
            done.append(file_name.replace(".done", ""))

    pending = [batch for batch in expected if batch not in done]
    print(f"Batches done: {sorted(done)} | still pending: {pending}")
    if pending:
        return "wait_for_more_batches"
    return "trigger_combine_dag"


# ----------------------------------------------------------------------------------------- DAG
with DAG(
    dag_id="swiggy_gstr1_batch_dag",
    description="GSTR-1 per batch: validate, fix, re-validate, upload file",
    start_date=datetime(2025, 4, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"period": "202506", "batch_id": "B1"},
    tags=["swiggy", "gstr1"],
) as dag:

    t_manifest   = PythonOperator(task_id="read_manifest",              python_callable=read_manifest)
    t_validate   = PythonOperator(task_id="gstr1_validation",           python_callable=gstr1_validation)
    t_fix        = PythonOperator(task_id="gstr1_error_fixing",         python_callable=gstr1_error_fixing)
    t_revalidate = PythonOperator(task_id="gstr1_revalidation",         python_callable=gstr1_revalidation)
    t_upload     = PythonOperator(task_id="gstr1_upload_file_creation", python_callable=gstr1_upload_file_creation)
    t_mark       = PythonOperator(task_id="mark_batch_complete",        python_callable=mark_batch_complete)
    t_check      = BranchPythonOperator(task_id="check_all_batches_done", python_callable=check_all_batches_done)
    t_trigger    = TriggerDagRunOperator(
        task_id="trigger_combine_dag",
        trigger_dag_id="swiggy_gstr1_combine_reco_dag",
        conf={"period": "{{ params.period }}"},
    )
    t_wait       = EmptyOperator(task_id="wait_for_more_batches")

    t_manifest >> t_validate >> t_fix >> t_revalidate >> t_upload >> t_mark >> t_check >> [t_trigger, t_wait]
