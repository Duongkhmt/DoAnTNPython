from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="vnstock_daily_sync",
    default_args=default_args,
    description="Sync stock data, compute indicators, then trigger AI prediction",
    schedule_interval="0 11 * * 1-5",
    catchup=False,
    tags=["vnstock", "daily", "ai"],
) as dag:
    run_sync = BashOperator(
        task_id="run_daily_sync",
        bash_command="python /opt/airflow/dags/test.py --daily",
    )

    compute_daily_indicators = BashOperator(
        task_id="compute_daily_indicators",
        bash_command="python /opt/airflow/dags/compute_indicators.py --daily",
    )

    run_sync >> compute_daily_indicators
