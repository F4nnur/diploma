import asyncio

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from includes.vs_modules.test import run_alerts

default_args = {
    'owner': 'airflow',
}


def call_alert():
    asyncio.run(run_alerts())


with DAG(
        dag_id='air_test',
        default_args=default_args,
        schedule_interval='*/10 * * * *',
        start_date=days_ago(0)
) as dag:
    run_alerts_task = PythonOperator(
        task_id='hello',
        python_callable=call_alert
    )

    run_alerts_task
