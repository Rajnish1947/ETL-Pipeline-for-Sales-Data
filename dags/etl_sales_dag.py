from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from scripts.extract_to_s3 import *
from scripts.transform_glue_job import *
from scripts.load_to_redshift import *

default_args = {'owner': 'rajnish', 'start_date': datetime(2025, 11, 1)}

dag = DAG('sales_etl_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False)

extract_task = PythonOperator(task_id='extract_to_s3', python_callable=lambda: exec(open('scripts/extract_to_s3.py').read()), dag=dag)
transform_task = PythonOperator(task_id='transform_glue', python_callable=lambda: exec(open('scripts/transform_glue_job.py').read()), dag=dag)
load_task = PythonOperator(task_id='load_to_redshift', python_callable=lambda: exec(open('scripts/load_to_redshift.py').read()), dag=dag)

extract_task >> transform_task >> load_task
