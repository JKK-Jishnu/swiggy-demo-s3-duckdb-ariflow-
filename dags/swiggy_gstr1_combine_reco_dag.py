"""
=====================================================================================================
DAG 2    : swiggy_gstr1_combine_reco_dag             GSTR-1 track, one run per month
-----------------------------------------------------------------------------------------------------
TRIGGER  : started by DAG 1 when the last batch of the month is done (conf {"period": "202506"}),
           or manually in the Airflow UI with config {"period": "202506"}

FLOW
  combine_batches                Activity 4 - all batches -> one sales dataset
     -> einv_vs_sales_reco       Activity 5 - e-invoice (IRN) vs sales      } run in parallel
     -> ewb_vs_sales_reco        Activity 6 - e-way bill vs sales           }

Each task only calls the job's run() function - all business logic lives in activities/<job>/job.py
=====================================================================================================
"""
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")


def _period(context) -> str:
    """Period from the trigger config (DAG 1) or, for a manual run, from the DAG params."""
    conf = context["dag_run"].conf or {}
    return conf.get("period") or context["params"]["period"]


def combine_batches(**context):
    from activities.combine_batches.job import run
    return run(_period(context))


def einv_vs_sales_reco(**context):
    from activities.einv_vs_sales_reco.job import run
    return run(_period(context))


def ewb_vs_sales_reco(**context):
    from activities.ewb_vs_sales_reco.job import run
    return run(_period(context))


with DAG(
    dag_id="swiggy_gstr1_combine_reco_dag",
    description="GSTR-1 per month: combine batches, EINV and EWB reconciliation",
    start_date=datetime(2025, 4, 1),
    schedule=None,
    catchup=False,
    params={"period": "202506"},
    tags=["swiggy", "gstr1", "reco"],
) as dag:

    t_combine = PythonOperator(task_id="combine_batches",    python_callable=combine_batches)
    t_einv    = PythonOperator(task_id="einv_vs_sales_reco", python_callable=einv_vs_sales_reco)
    t_ewb     = PythonOperator(task_id="ewb_vs_sales_reco",  python_callable=ewb_vs_sales_reco)

    t_combine >> [t_einv, t_ewb]
