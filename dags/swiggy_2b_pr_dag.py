"""
=====================================================================================================
DAG 3    : swiggy_2b_pr_dag                          Purchase register / GSTR-2B track, per month
-----------------------------------------------------------------------------------------------------
TRIGGER  : manually in the Airflow UI with config {"period": "202506"},
           once the purchase register and the GSTR-2B downloads for the month are in raw/

FLOW
  pr_validation                  Activity 7 - validate the purchase register      } run in parallel
  gstr2b_consolidation           Activity 8 - all 2B workbooks -> one table        }
     -> gstr2b_vs_pr_reco        Activity 9 - GSTR-2B vs purchase register, ITC at risk

  pr_validation also writes final/<period>/pr_errors_for_client.xlsx. If the client sends a
  corrected purchase register, upload it to raw/<period>/pr/ and run this DAG again.

Each task only calls the job's run() function - all business logic lives in activities/<job>/job.py
=====================================================================================================
"""
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")


def _period(context) -> str:
    conf = context["dag_run"].conf or {}
    return conf.get("period") or context["params"]["period"]


def pr_validation(**context):
    from activities.pr_validation.job import run
    return run(_period(context))


def gstr2b_consolidation(**context):
    from activities.gstr2b_consolidation.job import run
    return run(_period(context))


def gstr2b_vs_pr_reco(**context):
    from activities.gstr2b_vs_pr_reco.job import run
    return run(_period(context))


with DAG(
    dag_id="swiggy_2b_pr_dag",
    description="PR / GSTR-2B per month: validate PR, consolidate 2B, 2B vs PR reconciliation",
    start_date=datetime(2025, 4, 1),
    schedule=None,
    catchup=False,
    params={"period": "202506"},
    tags=["swiggy", "pr", "gstr2b", "reco"],
) as dag:

    t_pr    = PythonOperator(task_id="pr_validation",        python_callable=pr_validation)
    t_2b    = PythonOperator(task_id="gstr2b_consolidation", python_callable=gstr2b_consolidation)
    t_reco  = PythonOperator(task_id="gstr2b_vs_pr_reco",    python_callable=gstr2b_vs_pr_reco)

    [t_pr, t_2b] >> t_reco
