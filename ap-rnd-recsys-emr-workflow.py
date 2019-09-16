import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
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
    'owner' : 'RND-KangSeokWoo',
    'start_date' : airflow.utils.dates.days_ago(1), 
    'email' : ['pko954@amorepacific.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5)
}

dag = DAG( dag_id = 'rdb-emr-test', 
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
    vpc_security =[config["AWS"]["RDS"]["SECURITY_GROUP"]]
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
    
    context['task_instance'].xcom_push(key='ap-recsys-apmall-rds-airflow', value=cluster_id)

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


# EMR Function
def get_emr_client():
    session = boto3.session.Session(region_name='ap-northeast-2')
    emr_client = session.client('emr')
    return emr_client

def run_emr_cluster(**context):
    config = Variable.get(key='recommendation', deserialize_json=True)

    SHELL=config["AWS"]['EMR']['SHELL']
    
    # EMR step params
    EMR_BUCKET_NAME=config["AWS"]['EMR']['EMR_BUCKET_NAME']
    RDB_ENDPOINT= "jdbc:mysql://" + config["AWS"]['RDS']['HOST']
    RDB_DATABASE=config["AWS"]['RDS']['DB_NAME']
    RDB_TABLE=config["AWS"]['EMR']['RDB_TABLE']
    RDB_USER=config["AWS"]['RDS']['MASTER_USERNAME']
    RDB_PWD=config["AWS"]['RDS']['MASTER_USERPW']
    SOURCE_BUCKET=config["AWS"]['EMR']['SOURCE_BUCKET']
    DAYS_AGO=config["AWS"]['EMR']['DAYS_AGO']

    LOG_URL=config["AWS"]['EMR']['LOG_URL']
    SUBNET_ID=config["AWS"]['EMR']['SUBNET_ID']
    RELEASE_LABEL=config["AWS"]['EMR']['RELEASE_LABEL']
    MASTER_INTANCE_TYPE=config["AWS"]['EMR']['MASTER_INTANCE_TYPE']
    MASTER_INSTANCE_COUNT=config["AWS"]['EMR']['MASTER_INSTANCE_COUNT']
    SLAVE_INTANCE_TYPE=config["AWS"]['EMR']['SLAVE_INTANCE_TYPE']
    SLAVE_INSTANCE_COUNT=config["AWS"]['EMR']['SLAVE_INSTANCE_COUNT']
    
    try:
        emr_client = get_emr_client()
        response = emr_client.run_job_flow(Name='ap-recsys-apmall-emr-airflow',
                                                LogUri=LOG_URL,
                                                ReleaseLabel=RELEASE_LABEL,
                                                ServiceRole='EMR_DefaultRole',
                                                JobFlowRole='EMR_EC2_DefaultRole',
                                                VisibleToAllUsers=True,
                                                Applications=[{'Name': 'Spark'}],
                                                Steps=[{
                                                    'Name': 'etl',
                                                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                                                    'HadoopJarStep': {
                                                        'Jar': 's3://ap-northeast-2.elasticmapreduce/libs/script-runner/script-runner.jar',
                                                        'Args': [SHELL, EMR_BUCKET_NAME, RDB_ENDPOINT, RDB_DATABASE, RDB_TABLE, RDB_USER, RDB_PWD, SOURCE_BUCKET, DAYS_AGO]
                                                    }
                                                }],
                                                Instances={
                                                    'InstanceGroups': [
                                                        {
                                                            'Name': 'master',
                                                            'Market': 'SPOT',
                                                            'InstanceRole': 'MASTER',
                                                            'InstanceType': MASTER_INTANCE_TYPE,
                                                            'InstanceCount': MASTER_INSTANCE_COUNT
                                                        },
                                                        {
                                                            'Name': 'slave',
                                                            'Market': 'SPOT',
                                                            'InstanceRole': 'CORE',
                                                            'InstanceType': SLAVE_INTANCE_TYPE,
                                                            'InstanceCount': SLAVE_INSTANCE_COUNT
                                                        }],

                                                    'KeepJobFlowAliveWhenNoSteps': False,
                                                    'TerminationProtected': False,
                                                    'Ec2SubnetId': SUBNET_ID
                                                },
                                                Configurations=[
                                                        {
                                                            "Classification": "spark-env",
                                                            "Configurations": [
                                                                {
                                                                    "Classification": "export",
                                                                    "Properties": {
                                                                        "PYSPARK_PYTHON": "/usr/bin/python3",
                                                                        "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                                                                        }
                                                                }
                                                            ]
                                                        },
                                                        {
                                                            "Classification": "spark-defaults",
                                                            "Properties": {
                                                                "spark.sql.execution.arrow.enabled": "true"
                                                            }
                                                        },
                                                        {
                                                            "Classification": "spark",
                                                            "Properties": {
                                                                "maximizeResourceAllocation": "true"
                                                            }
                                                        }
                                                    ])
        
        JobFlowId = response['JobFlowId']
    except Exception as ex:
        raise AirflowException(f"Unexpected Error: {str(ex)}")

    context['task_instance'].xcom_push(key='ap-recsys-apmall-emr-airflow', value=JobFlowId)
    logger.info(f'[run_emr_cluster] JobFlowId: {JobFlowId} ***********')



# Sensor Function
class EmrJobFlowSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(EmrJobFlowSensor, self).__init__(*args, **kwargs)
        session = boto3.session.Session(region_name='ap-northeast-2')

        self.emr_client = session.client('emr')
          
    def poke(self, context):
        job_flow_id = context['task_instance'].xcom_pull(key='ap-recsys-apmall-emr-airflow')
        
        logger.info(f'[poke - EmrJobFlowSensor] job_flow_id: {job_flow_id} ***********')
        description = self.emr_client.describe_cluster(ClusterId=job_flow_id)
        
        #plogger.info(description)
        
        state = description['Cluster']['Status']['State']
        
        logger.info(f'[poke - EmrJobFlowSensor] state: {state} ***********')

        if state == "TERMINATED":
            logger.info(f'[poke - EmrJobFlowSensor] return True')
            return True
        elif state == "TERMINATED_WITH_ERRORS":
            raise AirflowException("Terminated with errors")
        else:
            logger.info(f'[poke - EmrJobFlowSensor] return False')
            return False

class RDSAvailableSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(RDSAvailableSensor, self).__init__(*args, **kwargs)
        self.rds_client = get_rds_client()

    def poke(self, context):
        cluster_identifier= context['task_instance'].xcom_pull(key='ap-recsys-apmall-rds-airflow')

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

# EMR Operator

run_emr_cluster_op = PythonOperator(
    task_id='run_emr_cluster',
    provide_context=True,
    python_callable=run_emr_cluster,
    dag=dag)

emr_jobflow_sensor = EmrJobFlowSensor(
    task_id='emr_sensor_task', 
    poke_interval=30, 
    dag=dag)

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
configure_op >> create_rds_op >> rds_available_sensor >> run_emr_cluster_op >> emr_jobflow_sensor >> delete_rds_op