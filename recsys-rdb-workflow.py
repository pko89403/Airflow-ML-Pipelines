import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sqlite_operator import SqliteOperator
# from airflow.operators.postgres_operator import PostgresOperator # when airflow_db connected by postgresql
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
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

HEADER = "Airflow" # os.environ('HEADER')

# DAG SETTINGS
default_args = {
    'owner' : 'KangSeokWoo',
    'start_date' : airflow.utils.dates.days_ago(1), 
    'email' : ['pko954@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5)
}

dag = DAG( dag_id = 'rdb-test', 
           default_args=default_args,
           schedule_interval='@daily')

# Boto3 API 
# RDS Functio

def push_all_parameter(**kwargs):
    #HEADER="DEV" # os.environ('AIRFLOW_ENV_NAME')

    # 1. download config file from s3 
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
    # 2. config()
    config_file = open(destination)
    config_data = json.load(config_file)

    # 3. push all parameters to XCOM
    Variable.set(key='recommendation', value=config_data, serialize_json=True)

def get_rds_client():
    rds_client = boto3.client(service_name='rds')
    return rds_client

def create_cluster(**context):
    config = Variable.get(key='recommendation', deserialize_json=True)

    cluster_id = config["AWS"]["RDS"]["CLUSTER_ID"]
    parameter_group_name = config["AWS"]["RDS"]["PARAMETER_GROUP"]
    db_name = config["AWS"]["RDS"]["DB_NAME"]
    vpc_security =["PIPE-RND-RDS"] #[config["AWS"]["RDS"]["SECURITY_GROUP"]]
    db_engine = config["AWS"]["RDS"]["ENGINE"]
    db_engine_version = config["AWS"]["RDS"]["ENGINE_VERSION"]
    db_engine_mode = config["AWS"]["RDS"]["ENGINE_MODE"]
    master_id = config["AWS"]["RDS"]["MASTER_USERNAME"]
    master_pw = config["AWS"]["RDS"]["MASTER_USERPW"]
    db_subnetgroup = config["AWS"]["RDS"]["SUBNET_GROUP_NAME"]

    
    rds_client = get_rds_client()
    try:
        response = rds_client.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            DBClusterParameterGroupName=parameter_group_name,
            DatabaseName=db_name,
            VpcSecurityGroupIds=vpc_security,
            Engine=db_engine,
            EngineVersion=db_engine_version,
            EngineMode=db_engine_mode,
            MasterUsername=master_id,
            MasterUserPassword=master_pw,
            DBSubnetGroupName=db_subnetgroup
        )

        logger.info(response)
        """
        response = rds_client.create_db_instance(
            DBClusterIdentifier=config["AWS"]["RDS"]["CLUSTER_ID"],
            DBInstanceIdentifier=config["AWS"]["RDS"]["DB_INSTANCE_ID"],
            DBInstanceClass=config["AWS"]["RDS"]["DB_INSTANCE_CLASS"],
            Engine=config["AWS"]["RDS"]["ENGINE"],
            DBSubnetGroupName=config["AWS"]["RDS"]["SUBNET_GROUP_NAME"],
            PubliclyAccessible=True
        )
        logger.info(response)
        """

    except Exception as ex:
        if 'DBClusterAlreadyExistsFault' in str(ex):
            delete_cluster()
            raise AirflowException(f"DBClusterAlreadyExistsFault: {str(ex)}")
        else:
            raise AirflowException(f"Unexpected Error: {str(ex)}")
    
    context['task_instance'].xcom_push(key='recsys-rds-airflow', value=cluster_id)

def delete_cluster():
    config = Variable.get(key='recommendation', deserialize_json=True)
    
    cluster_id = config["AWS"]["RDS"]["CLUSTER_ID"]
    skip_final_snapshot = config["AWS"]["RDS"]["SKIP_FINAL_SNAPSHOT"]


    rds_client = get_rds_client()
    """
    response = rds_client.delete_db_instance(
        DBInstanceIdentifier=config["AWS"]["RDS"]["DB_INSTANCE_ID"],
        SkipFinalSnapshot=True
    )
    logger.info(response)
    """
    response = rds_client.delete_db_cluster(
        DBClusterIdentifier=cluster_id,
        SkipFinalSnapshot=skip_final_snapshot
    )
    logger.info(response)


# Sensor Function
class RDSAvailableSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(RDSAvailableSensor, self).__init__(*args, **kwargs)
        self.rds_client = get_rds_client()

    def poke(self, context):
        cluster_identifier= context['task_instance'].xcom_pull(key='recsys-rds-airflow')

        try:
            response = self.rds_client.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
            logger.info(response)
            status = response['DBClusters'][0]['Status']
            logger.info(f"status: {status}")

            if status == 'available':
                return True
            else:
                return False
        except Exception as ex:
            if 'DBClusterNotFoundFault' in str(ex):
                return False
            else:
                raise AirflowException(f"Unexpected Error: {str(ex)}")

# Task Operator
configure_op = PythonOperator(
    task_id='configuration',
    python_callable=push_all_parameter,
    dag=dag
)

# RDS Operoator
create_rds_op = PythonOperator(
    task_id = 'create_rds_op',
    provide_context=True,
    python_callable = create_cluster,
    dag = dag
)

rds_available_sensor = RDSAvailableSensor(
    task_id='rds_available_sensor_task', 
    poke_interval=30, 
    dag=dag)

delete_rds_op = PythonOperator(
    task_id = "delete_rds_op",
    python_callable = delete_cluster,
    dag = dag
)

# Workflow Streaming Initialization
configure_op >> create_rds_op >> rds_available_sensor >> delete_rds_op