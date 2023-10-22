import sys
import os

import logging
from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
# from create_csvs import test_func
# import create_csvs
import get_surf_data
import snowflake_ingest
import execute_experiments
import preprocess_data
from airflow.operators.bash import BashOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": datetime(2023,10,13,2)}

dag = DAG(
    dag_id="snowflake_connector3", default_args=args, schedule_interval="@daily"
)


with dag:
    
    save_csvs = PythonOperator(task_id="save_csvs_task", python_callable=get_surf_data.save_csvs, op_kwargs={'spot_id': "5842041f4e65fad6a7708852"})
    setup_snowflake = PythonOperator(task_id="setup_snowflake_task", python_callable=snowflake_ingest.set_up)
    update_data = PythonOperator(task_id="update_data_task", python_callable=snowflake_ingest.update_db)
    preprocess_data = PythonOperator(task_id="preprocess_data_task", python_callable=snowflake_ingest.preprocess_and_store)
    execute_ex = PythonOperator(task_id="execute_ex_task", python_callable=execute_experiments.execute_experiment)

# setup_snowflake

save_csvs >> update_data >> preprocess_data >> execute_ex

# execute_ex