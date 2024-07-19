from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 17),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # "on_failure_callback": ,
}

dag = DAG("github-archive-pipeline", 
          default_args=default_args, 
          max_active_runs=1, 
          catchup=False, 
          tags=['data'])

# 데이터 저장
dt = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

download_data = BashOperator(
    task_id='download-data',
    bash_command=f"/opt/airflow/jobs/download-data.sh {dt} ",
    dag=dag
)

# spark-submit with /bin/bash
filename = '/opt/airflow/jobs/main.py'
filter_data = BashOperator(
    task_id='filter-data',
    bash_command=f'/opt/airflow/jobs/spark-submit.sh {filename}',
    dag=dag
)

download_data >> filter_data
