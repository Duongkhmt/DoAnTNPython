from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Arguments mặc định cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Khởi tạo DAG
with DAG(
    'vnstock_daily_sync',
    default_args=default_args,
    description='DAG tự động crawl dữ liệu chứng khoán VNStock mỗi ngày',
    # Chạy vào lúc 18:00 (6 PM) hàng ngày, từ thứ 2 đến thứ 6 (sau giờ giao dịch)
    # Lưu ý: Giờ này trên Airflow thường là UTC, bạn cần tính toán lại theo timezone nếu server để UTC.
    # Nếu server giờ VN (UTC+7), '0 18 * * 1-5' là 18:00 giờ VN.
    schedule_interval='0 11 * * 1-5', 
    catchup=False, # Không chạy lại các ngày trong quá khứ nếu bỏ lỡ
    tags=['vnstock', 'daily'],
) as dag:

    # Task chạy script crawl
    # Lưu ý: Cần đảm bảo môi trường chạy Airflow có đường dẫn đúng tới file test.py 
    # và có sẵn thư viện python (vnstock_data, pandas, psycopg2, timescale_utils...)
    
    # Ở đây sử dụng đường dẫn tuyệt đối theo máy của bạn, nếu Airflow chạy trên Docker 
    # hay máy ảo Linux, bạn cần thay đổi lại đường dẫn bash_command cho đúng.
    run_sync = BashOperator(
        task_id='run_daily_sync',
        bash_command='python /opt/airflow/dags/test.py --daily ',
    )

    trigger_ai_prediction = BashOperator(
        task_id='trigger_ai_prediction',
        # Dùng host.docker.internal để một container Docker (Airflow) có thể chọc ra hệ thống Windows Host bên ngoài
        bash_command='curl -X POST http://host.docker.internal:8000/daily_predict_all',
    )
    
    # Thứ tự chạy: Chạy crawl Data xong xuôi -> Đánh thức API của AI dậy để dự báo
    run_sync >> trigger_ai_prediction

