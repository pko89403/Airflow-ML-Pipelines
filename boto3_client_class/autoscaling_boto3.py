import boto3
import os


class AutoScaling(object):
    def __init__(self):
        self.serviceNamespace = 'sagemaker'
        self.client = boto3.client('application-autoscaling')
    
    def _describe_scalability(self, endpointName, variantName):
        endpoint = os.path.join('endpoint', endpointName)
        variant = os.path.join('variant', variantName)
        resourceID = os.path.join(endpoint, variant)

        
        response = self.client.describe_scalable_targets(
            ServiceNamespace = self.serviceNamespace,
            ResourceIds = [resourceID]
        )
        return response
    
    def _register_autoScaling(self, endpointName, variantName, minCapacity, maxCapacity, roleARN):
        endpoint = os.path.join('endpoint', endpointName)
        variant = os.path.join('variant', variantName)
        resourceID = os.path.join(endpoint, variant)
        
        response = self.client.register_scalable_target(
            ServiceNamespace = self.serviceNamespace,
            ResourceId = resourceID,
            ScalableDimension = 'sagemaker:variant:DesiredInstanceCount',
            MinCapacity=minCapacity,
            MaxCapacity=maxCapacity,
            RoleARN=roleARN
        )
        
        print( response )
        return response 