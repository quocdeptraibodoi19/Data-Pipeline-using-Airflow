from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from data_processing import run_elt

default_args = {
    "owner": "Nguyen Dinh Quoc",
    "depends_on_past": False,
    "start_date": datetime(year=2023, day=14, month=1),
    "email": ["quocthogminhqtm@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=4)
}

dag = DAG(
    "twitter_etl_dag",
    default_args=default_args,
    description="This is the first time I deploy the airflow on AWS",
)

# The comma at the final parameter is just for the convention since it increases the maintanability and readability 
run_etl = PythonOperator(
    task_id = "running_twitter_etl",
    python_callable= run_elt,
    dag=dag,
)
