import boto3
from datetime import datetime
import json
import time

class Sagemaker(object):
    def __init__(self):
        self.client = boto3.client('sagemaker')
        
    def _create_training_job(self, jobName, imageARN, roleARN, s3Path, instanceType, instanceCount, securityGroups, subNets, volumeSize, time):
        # make job name
        dt = datetime.now().isoformat().replace(":", "-").split('.')[0]  
        trainName = jobName + '-' + dt
        
        response = self.client.create_training_job(
            TrainingJobName=trainName,
            AlgorithmSpecification={
                'TrainingImage': imageARN,
                'TrainingInputMode' : 'Pipe'
            },
            RoleArn=roleARN,
            OutputDataConfig={
                'S3OutputPath': s3Path
            },
            ResourceConfig={
                'InstanceType': instanceType,
                'InstanceCount': instanceCount,
                'VolumeSizeInGB': volumeSize,
            },
            VpcConfig={
                'SecurityGroupIds' : securityGroups,
                'Subnets' : subNets
            },
            StoppingCondition={
                'MaxRuntimeInSeconds': time
            },
            EnableNetworkIsolation=False,
            EnableInterContainerTrafficEncryption=False
        )
        
        return response
    
    def _if_exists_delete_model(self, modelName):
        for model in self.client.list_models()['Models']:
            if(model['ModelName'] == modelName):
                self.client.delete_model( ModelName = modelName)
                time.sleep(180)
                break
    
    def _create_model(self, modelName, imageARN, envDict, roleARN, securityGroups, subNets):
        self._if_exists_delete_model(modelName)
        
        response = self.client.create_model(
            ModelName=modelName,
            PrimaryContainer={
                'ContainerHostname': 'PrimaryContainer',
                'Image': imageARN,
                'Environment': envDict
            },
            ExecutionRoleArn=roleARN,
            
            VpcConfig={
                'SecurityGroupIds' : securityGroups,
                'Subnets' : subNets
            },
            
            EnableNetworkIsolation=False
        )
        
        return response
    
    def _if_exists_delete_endpoint_conf(self, configName):
        for config in self.client.list_endpoint_configs()['EndpointConfigs']:
            if(config['EndpointConfigName'] == configName):
                self.client.delete_endpoint_config( EndpointConfigName=configName )
                time.sleep(180)
                break

    def _create_endpoint_config(self, configName, modelName, instanceType, initInstanceCnt, variantName):
        dt = datetime.now().isoformat().replace(":", "-").split('.')[0]  
        configName = configName + '-' + dt

        response = self.client.create_endpoint_config(
            EndpointConfigName=configName,
            ProductionVariants=[
                {
                    'VariantName': variantName,
                    'ModelName': modelName,
                    'InitialInstanceCount': initInstanceCnt,
                    'InstanceType': instanceType
                    #'AcceleratorType': accelatorType # We can't use this spec
                },
            ]
        )
        return response

    def _is_endpoint_exists(self, endPointName):
        responses = self.client.list_endpoints(NameContains=endPointName)

        for response in responses['Endpoints']:
            if( response['EndpointName'] == endPointName ): return True
        
        return False
        
    def _if_exists_delete_endpoint(self, endPointName):        
        if( self._is_endpoint_exists(endPointName) ):
            response = self.client.delete_endpoint(EndpointName=endPointName)
            time.sleep(180)

    def _create_endpoint(self, endpointName, configName):
        self._if_exists_delete_endpoint(endpointName)
        
        response = self.client.create_endpoint(
            EndpointName=endpointName,
            EndpointConfigName=configName,
        )
        return response

    def _update_endpoint_config(self, endPointName, configName):
        if( self._is_endpoint_exists(endPointName) == False):
            return False



        response = self.client.update_endpoint(
            EndpointName=endPointName,
            EndpointConfigName=configName
        )
        return response


    def _describe_endpoint(self, endPointName):
        if( self._is_endpoint_exists(endPointName) == False):
            return False

        response = self.client.describe_endpoint(EndpointName=endPointName)
        return response['EndpointStatus']


    def _create_serving(self, modelName, imageARN, envDict, roleARN, securityGroups, subNets, configName, initInstanceCnt, instanceType, endpointName, variantName):
        self._create_model(modelName, imageARN, envDict, roleARN, securityGroups, subNets)
        self._create_endpoint_config(configName, modelName, initInstanceCnt, instanceType, variantName)
        response = self._create_endpoint(endpointName, configName)
        return response