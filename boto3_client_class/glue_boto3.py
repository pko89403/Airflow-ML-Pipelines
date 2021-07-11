import boto3
import pprint
from time import sleep

print(boto3.__version__)

ACCESS_KEY = ""
SECRET_KEY = ""


def get_glue_client():
    glue_client = boto3.client(service_name='glue',
                               region_name='northeast-2',
                               endpoint_url='https://glue.northeast-2.amazonaws.com',
                               aws_access_key_id=ACCESS_KEY,
                               aws_secret_access_key=SECRET_KEY)
    return glue_client


def run_glue(job_name):
    glue_client = get_glue_client()
    glue_client.start_job_run(JobName=job_name)


def is_job_running(name):

    glue_client = get_glue_client()
    response = glue_client.get_job_runs(JobName=name)
    job_runs = response['JobRuns']
    for job in job_runs:
        if job['JobRunState'] == 'STARTING' or job['JobRunState'] == 'RUNNING':
            return True

    return False


def main():
    job_name = 'test-glue'
    run_glue(job_name)
    while True:
        print(is_job_running(job_name))
        sleep(1)


if __name__ is '__main__':
    main()
