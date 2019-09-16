import boto3
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class RDS():
    def __init__(self):
        self.client = boto3.client(service_name='rds')

    def rds_describe(self, instance_identifier):
        try:
            response = self.client.describe_db_instances(DBInstanceIdentifier=instance_identifier)
            logger.info(response)
        except Exception as ex:
            logger.error(f"Unexpected Error: {str(ex)}")

        finally:
            return response


    def check_cluster_available(self, cluster_identifier):
        # cluster_identifier = 'recsys-kwon'
        exist = False
        try:
            response = self.client.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
            if response['DBClusters'][0]['Status'] is 'available':
                exist = True
            else:
                exist = False

        except Exception as ex:
            if 'DBClusterNotFoundFault' in str(ex):
                exist = False
        finally:
            return exist


    def create_cluster(self, cluster_identifier, database_name, security_group, engine, engine_version, master_username, master_user_password, subnet_group_name):
        try:
            response = self.client.create_db_cluster(
                DBClusterIdentifier=cluster_identifier,
                DatabaseName=database_name,
                VpcSecurityGroupIds=security_group,
                Engine=engine,
                EngineVersion=engine_version,
                MasterUsername=master_username,
                MasterUserPassword=master_user_password,
                DBSubnetGroupName=subnet_group_name
            )
            logger.info(response)

        except Exception as ex:
            if 'DBClusterAlreadyExistsFault' in str (ex):
                logger.warn('delete db cluster already existed')
            else:
                logger.error(f"Unexpected Error: {str(ex)}")            


    def delete_cluster(self, cluster_identifier, skip_final_snapshot=True):
        try:
            response = self.client.delete_db_cluster(
                DBClusterIdentifier=cluster_identifier,
                SkipFinalSnapshot=skip_final_snapshot
            )
            logger.info(response)

        except Exception as ex:
            if 'DBClusterNotFoundFault' in str(ex):
                logger.error('db cluster already deleted')
            else:
                logger.error(f"Unexpected Error: {str(ex)}")
        
    def create_instance(self, cluster_identifier, db_identifier, instance_class, engine, subnet_group_name):
        try:
            response = self.client.create_db_instance(
                DBClusterIdentifier=cluster_identifier,
                DBInstanceIdentifier=db_identifier,
                DBInstanceClass=instance_class,
                Engine=engine,
                DBSubnetGroupName=subnet_group_name,
                PubliclyAccessible=True
            )

            logger.info(response)
        except Exception as ex:
            logger.error(f"Unexpected Error: {str(ex)}")

    def delete_instance(self, db_identifier, skip_final_snapshot=True):
        try:
            response = self.client.delete_db_instance(
                DBInstanceIdentifier=db_identifier,
                SkipFinalSnapshot=True
            )
            logger.info(response)
        except Exception as ex:
            logger.error(f"Unexpected Error: {str(ex)}")


