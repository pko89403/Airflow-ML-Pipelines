import boto3
import os # Subprocess Module Okay
import docker
import operator

from git import Repo # GitPython
import shutil



""" FOLLOW JUST A STEP of AWS ECR PUSH
DOCKERFILE_PATH = '.'
ECR_REPOSITORY = "rnd-recsys-image" # same as image name
REGION = 'northeast-2' # ecr located regions
IMAGE_SAVE_CNT = 3
CODE_URL = "https://git-codecommit.northeast-2.amazonaws.com/v1/repos/AmazonSageMaker-rnd-recsys-code"

os.system('$(aws ecr get-login --no-include-email --region northeast-2)')
os.system('docker build -t ' + ECR_REPOSITORY + ' ' + DOCKERFILE_PATH)
os.system('docker tag ' + ECR_REPOSITORY + ':latest ' + IMAGE_URI)
os.system('docker push ' + IMAGE_URI)
"""

class ECR(object):
    def __init__(self):
        self.client = boto3.client('ecr') # Need ECR role! 
        self.repository = None
        os.system('$(aws ecr get-login --no-include-email --region northeast-2 )')
    
    def _codeCommit_Clone(self, code_url):
        current = os.path.dirname( os.path.abspath(__file__) )
        target = os.path.join( current, "cloned" )

        if(os.path.exists(target)): shutil.rmtree(target)

        Repo.clone_from( code_url, target )
        return target


    def is_Repository_Exists(self, ecr_name):
        describes = self.client.describe_repositories()

        for repository in describes['repositories']:            
            if(ecr_name == repository['repositoryName']):                
                self.repository = repository
                return True
        return False

    def _create_Repository(self, ecr_name):
        if( not self.is_Repository_Exists(ecr_name) ):
            response = self.client.create_repository(repositoryName=ecr_name)
            self.repository = response['repository']
            return response['repository']
        return self.repository

    def _describe_images(self, ecr_name):
        if( self.is_Repository_Exists(ecr_name) ):
            response = self.client.describe_images(repositoryName=ecr_name)
            return response
    
    def _put_image(self, configfile_path, dockerfile_path, ecr_name):
        # copy config into dockerfile_path
        shutil.copy2(configfile_path, dockerfile_path)
        
        # make docker images
        docker_client = docker.from_env()
        docker_images_client = docker_client.from_env().images
        
        if( not self.is_Repository_Exists(ecr_name) ):
            self._create_Repository(ecr_name) 
        
        imageName = self.repository['repositoryUri']
        docker_images_client.build(path=dockerfile_path, tag=imageName)
        docker_images_client.push(imageName)

        # delete cloned file
        shutil.rmtree(dockerfile_path)
        return imageName


        
    def _batch_delete_image(self, repository, image_save_count):
        imageDetails = self._describe_images(repository)['imageDetails']
        
        imageDetails.sort(key = operator.itemgetter('imagePushedAt'))
        
        imageCnt = len(imageDetails)

        if( imageCnt >= image_save_count ):
                for idx in range(0, imageCnt - image_save_count):
                    self.client.batch_delete_image(
                        repositoryName=repository,
                        imageIds=[
                            {
                                'imageDigest': imageDetails[idx]['imageDigest'],
                            },
                        ]
                    )