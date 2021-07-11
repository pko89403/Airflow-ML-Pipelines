# ML Pipelines on Amazon AWS


## Python, Boto3, Airflow

- Recsys Pipeline
- SCP Pipeline
- EMR
- RDS
- SageMaker
- XCom

## Project Architecture
```
.
├── README.md
├── boto3_client_class
│   ├── autoscaling_boto3.py
│   ├── ecr_boto3.py
│   ├── emr_boto3.py
│   ├── glue_boto3.py
│   ├── rds_boto3.py
│   └── sagemaker_boto3.py
├── recsys-emr-workflow.py
├── recsys-rdb-workflow.py
├── recsys-sagemaker-workflow.py
├── recsys-workflow.py
└── recsys-xcom_delete.py
```

## Recsys Workflow Scenario ( SageMaker )
## SCP WorkFlow Scenario ( SCP is Simple Just on EC2 )
1. Get Config Info ( Anywhere )
2. Push Config Info into Airflow Variables 
3. Get Code From Personal Repo
4. Build DockerFile ( Build on Airflow)
5. Push Image into Image Repo
6. Run Image:Latest

## Issues
chmod +x /var/run/docker.sock <- DockerOperator를 사용하기 위해 dockerurl 지정 필요 및 실행 권한 변경 필요
Netowork Configuration 필요
