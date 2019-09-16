import os
import boto3
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class EMR():
    def __init__(self):
        self.client = boto3.client(service_name='emr')

    def run_job_flow(self, shell, emr_bucket, rdb_endpoint, db_name, db_table, db_id, db_pw, src_bucket, log_url, subnet, release_label, master_instance_type, master_instance_count, slave_instance_type, slave_instance_count):
        SHELL=shell
    
        # EMR step params
        EMR_BUCKET_NAME= emr_bucket
        RDB_ENDPOINT=rdb_endpoint
        RDB_DATABASE=db_name
        RDB_TABLE=db_table
        RDB_USER=db_id
        RDB_PWD=db_pw
        SOURCE_BUCKET=src_bucket
        LOG_URL=log_url
        SUBNET_ID=subnet
        RELEASE_LABEL=release_label
        MASTER_INTANCE_TYPE=master_instance_type
        MASTER_INSTANCE_COUNT=master_instance_count
        SLAVE_INTANCE_TYPE=slave_instance_type
        SLAVE_INSTANCE_COUNT=slave_instance_count
        
        try:
            response = self.client.run_job_flow(Name,
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
                                                        'Args': [SHELL, EMR_BUCKET_NAME, RDB_ENDPOINT, RDB_DATABASE, RDB_TABLE, RDB_USER, RDB_PWD, SOURCE_BUCKET]
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
            logger.error(f"Unexpected Error: {str(ex)}")

        logger.info(f'[run_emr_cluster] JobFlowId: {JobFlowId} ***********')
        return JobFlowId

    def is_job_completed(self, job_flow_id):
        try:
            description = self.client.describe_cluster(ClusterId=job_flow_id)
            
            logger.info(description)

            state = description['Cluster']['Status']['State']
        
            if state is "TERMINATED":
                return True
            else:
                return False
        except Exception as ex:
            logger.error(f"Unexpected Error: {str(ex)}")
        