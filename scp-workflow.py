import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.exceptions import AirflowException
from airflow.operators.docker_operator import DockerOperator
import docker
import time
# BOTO3
import boto3
import botocore

# LOGING
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# CONFIG FILE
from airflow.models import Variable
import os
import json

from git import Repo # GitPython
import shutil


DAG_ID = "SCP-WORKFLOW-MODEL-TEST"
HEADER = "Airflow" # os.environ('HEADER')

# DAG SETTINGS
default_args = {
    'owner' : 'Kangseokwoo',
    'start_date' : airflow.utils.dates.days_ago(1),
    'email' : ['pko954@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=1)
}

dag = DAG( dag_id = DAG_ID,
           default_args=default_args,
           schedule_interval='30 1 * * *')
#'0 11 * * *'
# Everyday at 11 AM.
# Boto3 API
# RDS Functio

def push_all_parameter(**kwargs):
    #HEADER="DEV" # os.environ('AIRFLOW_ENV_NAME')

    # 1. download config file from s3
    """
    bucketName = 'ap-rnd-recsys-test'
    at_S3_Path = 'AirflowConfig/'
    outputName = 'config.json'
    s3 = boto3.resource('s3')
    s3_client = boto3.client('s3')
    bucket = s3.Bucket(bucketName)
    current = os.path.dirname( os.path.abspath(__file__) )
    destination = os.path.join( current, outputName)
    if( os.path.isfile(destination)):
        os.remove(destination)
    for file in bucket.objects.filter(Prefix=at_S3_Path):
        print(file)
        s3_client.download_file(file.bucket_name, file.key, destination)
        print("Download File ... ", file.key)
    """
    current = os.path.dirname( os.path.abspath(__file__))
    destination='config.json'
    destination = os.path.join(current, destination)
    config_file = open(destination)
    config_data = json.load(config_file)

    # 3. push all parameters to XCOM
    Variable.set(key='scp', value=config_data, serialize_json=True)

def code_cloning(**kwargs):
    config = Variable.get(key='scp', deserialize_json=True)
    codecommit_uri = config["AWS"]["CODECOMMIT"]

    current = os.path.dirname( os.path.abspath(__file__))
    target = os.path.join('/', current)
    target = os.path.join(target, "cloned" )

    if os.path.exists(target) is True:
        shutil.rmtree(target)

    Repo.clone_from(codecommit_uri, target)

    dockerfile_path = target
    docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    docker_images_client = docker_client.from_env().images

    try:
        docker_images_client.remove(image='scp', force=True, noprune=False)
    except Exception as e:
        logger.debug(e)
    finally:
        imageName = 'scp'
        docker_images_client.build(path = dockerfile_path, tag = imageName)
        shutil.rmtree(dockerfile_path)

# Task Operator
configure_op = PythonOperator(
    task_id='configuration',
    python_callable=push_all_parameter,
    dag=dag
)
print("configure_op is done")

clone_code = PythonOperator(
    task_id='clone_repo',
    python_callable=code_cloning,
    provide_context=True,
    dag=dag
)

docker_run = DockerOperator(
    task_id = 'docker_cmd',
    image = 'scp:latest',
    api_version='auto',
    auto_remove=True,
    docker_url='unix:///var/run/docker.sock',
    network_mode='bridge',
    dag=dag
)

configure_op >> clone_code >> docker_run