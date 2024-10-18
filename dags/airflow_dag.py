from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from util.load_and_process_data import process_data, download_and_unzip_files

default_args = {
    'owner': 'xiaoyu',
    'start_date': datetime(2024, 10, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'airflow', default_args=default_args, schedule_interval=timedelta(days=1))

download_and_unzip_files_task = PythonOperator(
        task_id='download_and_unzip_files',
        python_callable=download_and_unzip_files,
        dag=dag)

process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        dag=dag)

download_and_unzip_files_task >> process_task