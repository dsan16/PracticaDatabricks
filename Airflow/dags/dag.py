import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from PracticaDatabricks.kaggle_csv import download_kaggle_dataset
from PracticaDatabricks.databricks_connection import Upload_and_runJob


with DAG(
    dag_id="news_dag_v2",
    start_date=pendulum.datetime(2025, 4, 1, tz="Europe/Madrid"),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    concurrency=1
) as dag:

    task_extract = PythonOperator(
        task_id="download_kaggle_dataset",
        python_callable=download_kaggle_dataset,
    )

    task_load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=Upload_and_runJob
    )

    dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=(
        'cd /opt/airflow/dbtLimpieza && '
        'dbt run'
    )

)
    
task_extract >> task_load >> dbt_run