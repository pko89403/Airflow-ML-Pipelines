# ML Pipelines on Amazon AWS


## Python, Boto3, Airflow

- Recommender Pipeline
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