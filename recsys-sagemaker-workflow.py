import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sqlite_operator import SqliteOperator
from boto3_client_class.sagemaker_boto3 import Sagemaker
from boto3_client_class.ecr_boto3 import ECR
from boto3_client_class.autoscaling_boto3 import AutoScaling
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

dag = DAG( dag_id = 'sagemaker-test', 
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
    kwargs['task_instance'].xcom_push(key='config_path', value=destination )

# ECR Function
def ecr_push(**kwargs):
    # get airflow variables 
    config = Variable.get(key='recommendation', deserialize_json=True)
    
    # include config.json into git cloned folder
    configfile_path = kwargs['task_instance'].xcom_pull(key='config_path')
    codecommit_uri = config["AWS"]['ECR']['CODE_URL']
    ecr_uri = config["AWS"]['ECR']['ECR_REPOSITORY']
    image_save_limit = config["AWS"]['ECR']['IMAGE_SAVE_CNT']

    ecrWorker = ECR()
    
    # clone model code repository
    dockerfile_path = ecrWorker._codeCommit_Clone(codecommit_uri)
    # copy config.json > docker build > ecr_push
    result = ecrWorker._put_image(  configfile_path = configfile_path,
                                    dockerfile_path = dockerfile_path,
                                    ecr_name = ecr_uri)
    # delete docker image count 
    ecrWorker._batch_delete_image(  repository = ecr_uri,
                                    image_save_count = image_save_limit)

    result = str(result)
    kwargs['task_instance'].xcom_push(key='ecr_url', value=(result+":latest") )
    return result

# Sagemaker Training Function
def create_training_job(**kwargs):
    config = Variable.get(key='recommendation', deserialize_json=True)

    job_name = config["AWS"]["SAGEMAKER"]["TRAININGJOB"]['JOB_NAME']
    image_arn = config["AWS"]["ECR"]['ECR_URL']
    role_arn = config["AWS"]["SAGEMAKER"]["COMMON"]['ROLE_ARN']
    s3_path = config["AWS"]["SAGEMAKER"]["TRAININGJOB"]['S3_OUTPUT_PATH']
    instance_type = config["AWS"]["SAGEMAKER"]["COMMON"]['INSTANCE']
    instance_cnt = config["AWS"]["SAGEMAKER"]["COMMON"]['INSTANCE_COUNT']
    securitygroups = config["AWS"]["SAGEMAKER"]["COMMON"]['SECURITYGROUP']
    subnets = config["AWS"]["SAGEMAKER"]["COMMON"]['SUBNET_PRI_A']
    volume = config["AWS"]["SAGEMAKER"]["TRAININGJOB"]['CONTAINER_VOLUME']
    time = config["AWS"]["SAGEMAKER"]["TRAININGJOB"]['TRAINING_SECOND']

    sageWorker = Sagemaker()
    result = sageWorker._create_training_job(   jobName=job_name,
                                                imageARN=image_arn,
                                                roleARN=role_arn,
                                                s3Path=s3_path,
                                                instanceType=instance_type,
                                                instanceCount=instance_cnt,
                                                securityGroups=securitygroups,
                                                subNets=subnets,
                                                volumeSize=volume,
                                                time=time)
    
    logger.info(result)
    jobName = result['TrainingJobArn'].split('/')[-1]
    kwargs['task_instance'].xcom_push(key='training_job_name', value=jobName)

# Sagemaker - Inference Function
def create_model(**kwargs):
    config = Variable.get(key='recommendation', deserialize_json=True)

    model_name = config["AWS"]["SAGEMAKER"]["MODEL"]["MODEL_NAME"]
    image_arn = config["AWS"]["ECR"]['ECR_URL']
    environ_var = { 'TRAIN_JOB_NAME' : kwargs['task_instance'].xcom_pull(task_ids='create_Training_Job', key='training_job_name') }
    role_arn = config["AWS"]["SAGEMAKER"]["COMMON"]["ROLE_ARN"]
    securitygroups = config["AWS"]["SAGEMAKER"]["COMMON"]['SECURITYGROUP']
    subnets = config["AWS"]["SAGEMAKER"]["COMMON"]['SUBNET_PRI_AC']


    sageWorker = Sagemaker()
    result = sageWorker._create_model(  modelName=model_name,
                                        imageARN=image_arn,
                                        envDict=environ_var,
                                        roleARN=role_arn,
                                        securityGroups=securitygroups,
                                        subNets=subnets)
    logger.info(result)

def create_endpoint_config(**kwargs):
    config = Variable.get(key='recommendation', deserialize_json=True)

    config_name = config["AWS"]["SAGEMAKER"]["ENDPOINT_CONFIGURE"]["CONFIGURE_NAME"]
    model_name = config["AWS"]["MODEL"]["MODEL_NAME"]
    instance_type = config["AWS"]["COMMON"]["INSTANCE"]
    initInstance_cnt = config["AWS"]["COMMON"]["INSTANCE_COUNT"]
    variant_name = config["AWS"]["SAGEMAKER"]["ENDPOINT_CONFIGURE"]["VARIANT_NAME"]

    sageWorker = Sagemaker()
    result = sageWorker._create_endpoint_config(    configName=config_name,
                                                    modelName=model_name,
                                                    instanceType=instance_type,
                                                    initInstanceCnt=initInstance_cnt,
                                                    variantName=variant_name)
    logger.info(result)
    configName = result['EndpointConfigArn'].split('/')[-1]
    kwargs['task_instance'].xcom_push(key='endpoint_config_name', value=configName)

def branch_check_endpoint(**kwargs):
    config = Variable.get(key='recommendation', deserialize_json=True)

    endpoint_name = config["AWS"]["SAGEMAKER"]["ENDPOINT"]["ENDPOINT_NAME"]
    kwargs['task_instance'].xcom_push(key='endpoint_name', value=endpoint_name)


    sageWorker = Sagemaker()
    result = sageWorker._is_endpoint_exists(    endPointName=endpoint_name)
    
    task_id = 'create_EndPoint'
    if( result == True ):
        status = sageWorker._describe_endpoint( endPointName=endpoint_name)
        

        logger.info(status)
        if( status == 'InService'):
            task_id = 'check_endpoint_inservice'


    return task_id

def create_endpoint(**kwargs):
    config = Variable.get(key='recommendation', deserialize_json=True)

    endpoint_name = config["AWS"]["SAGEMAKER"]["ENDPOINT"]["ENDPOINT_NAME"]
    kwargs['task_instance'].xcom_push(key='endpoint_name', value=endpoint_name)

    endpoint_configure_name=kwargs['task_instance'].xcom_pull(task_ids='create_EndPoint_Config', key='endpoint_config_name')    

    sageWorker = Sagemaker()
    result = sageWorker._create_endpoint(   endpointName=endpoint_name,
                                            configName=endpoint_configure_name)
    logger.info(result)


def update_endpoint_config(**kwargs):
    config = Variable.get(key='recommendation', deserialize_json=True)
    endpoint_name = config["AWS"]["SAGEMAKER"]["ENDPOINT"]["ENDPOINT_NAME"]

    endpoint_configure_name=kwargs['task_instance'].xcom_pull(task_ids='create_EndPoint_Config', key='endpoint_config_name')    

    sageWorker = Sagemaker()
    result = sageWorker._update_endpoint_config(   endPointName=endpoint_name,
                                                    configName=endpoint_configure_name)
    logger.info(result)

# AutoScaling configure Function
def endpoint_autoScaling_config():
    config = Variable.get(key='recommendation', deserialize_json=True)

    endpoint_name = config["AWS"]["SAGEMAKER"]["ENDPOINT"]["ENDPOINT_NAME"]
    variant_name = config["AWS"]["SAGEMAKER"]["ENDPOINT_CONFIGURE"]["VARIANT_NAME"]
    min_capacity = config["AWS"]["SAGEMAKER"]["ENDPOINT"]["MIN_CAPACITY"]
    max_capacity = config["AWS"]["SAGEMAKER"]["ENDPOINT"]["MAX_CAPACITY"]
    role_arn = config["AWS"]["COMMON"]["ROLE_ARN"]

    targeter = AutoScaling()
    result = targeter._register_autoScaling(    endpointName=endpoint_name,
                                                variantName=variant_name,
                                                minCapacity=min_capacity,
                                                maxCapacity=max_capacity,
                                                roleARN=role_arn)

    logger.info(result)

# Sensor Function
class TrainingStatusSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):        
        super(TrainingStatusSensor, self).__init__( *args, **kwargs)
        self.client = boto3.client('sagemaker')

    def poke(self, context):
        checkName = context['task_instance'].xcom_pull(task_ids='create_Training_Job', key='training_job_name')

        
        response = self.client.describe_training_job(TrainingJobName = checkName)
        checkVal = response['TrainingJobStatus']
        
        logger.info(f"Training job Status\t{checkVal}")

        if(checkVal == 'Stopped' or checkVal == 'Completed'):
            logger.info("Training job completed  ... ")
            return True
        elif checkVal == 'Failed':
            raise AirflowException('Training Job Failed')
        else:
            return False

class EndpointStatusSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(EndpointStatusSensor, self).__init__( *args, **kwargs)
        self.client = boto3.client('sagemaker')

    def poke(self, context):
        endpoint_name = context['task_instance'].xcom_pull(key='endpoint_name')
        response = self.client.describe_endpoint(EndpointName = endpoint_name)
        checkVal = response['EndpointStatus']

        logger.info(f"Sagemaker Endpoint Status\t{checkVal}")

        if(checkVal == 'InService'):
            logger.info("Sagemaker Endpoint Done  ... ")
            return True
        elif checkVal == 'Failed':
            raise AirflowException('Endpoint Status Failed')
        else:
            return False

# Task Operator
configure_op = PythonOperator(
    task_id='configuration',
    python_callable=push_all_parameter,
    provide_context=True,
    dag=dag
)

# ECR Operator
ecr_push_op = PythonOperator(
    task_id='ecr_push',
    python_callable=ecr_push,
    provide_context=True,
    dag=dag,
)    

# Sagemaker Training Operator
train_op = PythonOperator(
    task_id='create_Training_Job',
    python_callable=create_training_job,
    provide_context=True,
    dag=dag,
)

training_sensor = TrainingStatusSensor(
    task_id='check_Training_Job',
    #timeout=10,
    poke_interval=30, 
    dag=dag
)

# Sagemaker Inference Operator
model_op = PythonOperator(
    task_id='create_serving_model',
    python_callable=create_model,
    provide_context=True,
    dag=dag,
)

endpoint_config_op = PythonOperator(
    task_id='create_EndPoint_Config',
    python_callable=create_endpoint_config,
    provide_context=True,
    dag=dag
)

branch_endpoint_op = BranchPythonOperator(
    task_id='endpoint_exisits',
    python_callable=branch_check_endpoint,
    provide_context=True,
    dag=dag
)

endpoint_op = PythonOperator(
    task_id='create_EndPoint',
    python_callable=create_endpoint,
    provide_context=True,
    dag=dag
)

serving_sensor1 = EndpointStatusSensor(
    task_id='check_endpoint_inservice',
    #timeout=10,
    poke_interval=60,
    dag=dag
)

endpoint_config_update_op = PythonOperator(
    task_id='update_endpoint_config',
    python_callable=update_endpoint_config,
    provide_context=True,
    dag=dag
)

join_op = DummyOperator(
    task_id='join',
    trigger_rule='one_success',
    dag=dag
)

serving_sensor2 = EndpointStatusSensor(
    task_id='check_serving_endpoint',
    #timeout=10,
    poke_interval=60,
    dag=dag
)

autoscaling_define = PythonOperator(
    task_id='endpoint_autoScaling_policy_define',
    python_callable=endpoint_autoScaling_config,
    dag=dag
)

serving_sensor3 = EndpointStatusSensor(
    task_id='check_serving_endpoint2',
    #timeout=10,
    poke_interval=60,
    dag=dag
)

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
configure_op >> ecr_push_op >> train_op >> training_sensor >> model_op >> endpoint_config_op >> branch_endpoint_op
branch_endpoint_op >> endpoint_op >> join_op
branch_endpoint_op >> serving_sensor1 >> endpoint_config_update_op >> join_op
join_op >> serving_sensor2 >> autoscaling_define >> serving_sensor3 >> delete_xcom_task