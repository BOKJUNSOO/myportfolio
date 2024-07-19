from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta


file_path = "/opt/bitnami/spark/data/movies.csv"    # data는 무조건 spark에서 읽기

now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],   # dummy email
    "email_on_failure": False,  # 알림
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="hello-world", 
        description="write description here",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job = SparkSubmitOperator(        # 단순한 submit은 sparkoperator로 가능
    task_id="spark_job",                # 복잡한 config 는 shell 파일 작성 필요
    application="/jobs/hello-world.py",  # 파이썬 파일 경로필요 - 상대경로 compose volume 참고~
    name="HelloWorld",
    conn_id="spark-conn",               # Admin connection
    verbose=1,
    application_args=[file_path],
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end
