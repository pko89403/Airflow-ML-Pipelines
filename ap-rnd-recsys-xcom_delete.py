import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.postgres_operator import PostgresOperator # when airflow_db connected by postgresql
from airflow.exceptions import AirflowException
import time

# DAG SETTINGS
default_args = {
    'owner' : 'RND-KangSeokWoo',
    'start_date' : airflow.utils.dates.days_ago(1), 
    'email' : ['pko954@amorepacific.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5)
}

dag = DAG( dag_id = 'xcom-msg-delete-test', 
           default_args=default_args,
           schedule_interval='@daily')


# XCOM Delete Operator
delete_xcom_task = SqliteOperator(
    task_id='delete_xcom_task',
    sqlite_conn_id='airflow_db',
    sql="delete from xcom where dag_id='{}'".format('recsys-personalize-batch'),
    dag=dag
)

# Turbine use PostgreSQL  
"""
delete_xcom_task = PostgresOperator(
      task_id='delete-xcom-task',
      postgres_conn_id='airflow_db',
      sql="delete from xcom where dag_id='{}'".format(config["AWS"]["AIRFLOW"]["DAG_ID"]),
      dag=dag)
"""


# Workflow Streaming Initialization
delete_xcom_task