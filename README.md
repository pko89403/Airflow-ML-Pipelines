# Amazon AWS로 구축하는 머신러닝의 모든 것
## 사용언어 python, 라이브러리 : Amazon AWS Python API : boto3, Airflow

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