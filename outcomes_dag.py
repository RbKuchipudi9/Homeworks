import os
import sys
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


from etl_files.data_transform import dimensionalmodeling
from etl_files.data_api_to_gcp import main
from etl_files.data_to_postgres_gcp import load_data_to_postgres_main


default_args = {
    "owner": "charan",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}


with DAG(
    dag_id="lab3_dag",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
        start = BashOperator(task_id = "START", bash_command = "echo start")

        api_data_to_gcp =  PythonOperator(task_id = "API_DATA_TO_GCP", python_callable = main,)

        dimensional_modeling = PythonOperator(task_id="DIMENSIONAL_MODELING", python_callable=dimensionalmodeling,)

        load_data_animal_dim = PythonOperator(task_id="LOAD_DATA_ANIMAL_DIM", python_callable=load_data_to_postgres_main, op_kwargs={"file_name": 'data_animal_dim.csv', "table_name": 'data_animal_dim'},)

        load_data_outcome_dim = PythonOperator(task_id="LOAD_DATA_OUTCOME_DIM", python_callable=load_data_to_postgres_main, op_kwargs={"file_name": 'data_outcome_dim.csv', "table_name": 'data_outcome_dim'},)
        
        load_data_time_dim = PythonOperator(task_id="LOAD_DATA_TIME_DIM", python_callable=load_data_to_postgres_main, op_kwargs={"file_name": 'data_time_dim.csv', "table_name": 'data_time_dim'},)
        
        load_data_animal_fact = PythonOperator(task_id="LOAD_DATA_ANIMAL_FACT", python_callable=load_data_to_postgres_main, op_kwargs={"file_name": 'data_animal_fact.csv', "table_name": 'data_animal_fact'},)
        
        end = BashOperator(task_id = "END", bash_command = "echo end")

        start >> api_data_to_gcp >> dimensional_modeling >> [load_data_animal_dim, load_data_outcome_dim, load_data_time_dim] >> load_data_animal_fact >> end
        