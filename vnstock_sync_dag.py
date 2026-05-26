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
    description="Sync stock data, index data, compute indicators, then trigger AI prediction",
    schedule_interval="0 11 * * 1-5", # 18:00 giờ Việt Nam (T2-T6)
    catchup=False,
    tags=["vnstock", "daily", "ai"],
) as dag:

    # 1. Task cào đa luồng toàn bộ cổ phiếu đơn lẻ
    run_sync = BashOperator(
        task_id="run_daily_sync",
        bash_command="python /opt/airflow/dags/crawler_pipeline.py --daily",
    )

    # 2. Task cào chỉ số chung VNINDEX (Mới thêm vào)
    sync_vnindex = BashOperator(
        task_id="sync_vnindex",
        bash_command="python /opt/airflow/dags/sync_vnindex.py",
    )

    # 3. Task tính toán chỉ báo kỹ thuật tổng hợp
    compute_daily_indicators = BashOperator(
        task_id="compute_daily_indicators",
        bash_command="python /opt/airflow/dags/compute_indicators.py --daily",
    )

    # Thiết lập luồng: Cào xong cả Cổ phiếu & VNINDEX mới tiến hành tính chỉ báo
    [run_sync, sync_vnindex] >> compute_daily_indicators
