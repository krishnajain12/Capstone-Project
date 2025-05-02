from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import time

# AWS region and Glue job list
REGION = 'us-east-1'
GLUE_JOBS = [
    'poc-bootcamp-group-3-employee-data',
    'poc-bootcamp-group-3-employee-timeframe-data',
    'poc-bootcamp-group-3-employee-leave-data',
    'poc-bootcamp-group-3-count_by_designation',
    'poc-bootcamp-group-3-threshold'
]

# Function to trigger and wait for Glue job to complete
def trigger_glue_job(job_name):
    client = boto3.client('glue', region_name=REGION)

    response = client.start_job_run(JobName=job_name)
    job_run_id = response['JobRunId']
    print(f"Started Glue job: {job_name}, Run ID: {job_run_id}")

    # Wait and poll status every 30 seconds
    while True:
        run_state = client.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']['JobRunState']
        print(f"Job {job_name} current state: {run_state}")

        if run_state in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
            break
        time.sleep(30)

    # Handle final state
    if run_state != 'SUCCEEDED':
        raise Exception(f"Glue job {job_name} failed with state: {run_state}")
    print(f"Glue job {job_name} completed successfully.")

# Default task settings
default_args = {
    'owner': 'krishna',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Define the DAG
with DAG(
    dag_id='daily_employee_glue_jobs_sequential',
    default_args=default_args,
    description='Triggers 5 AWS Glue jobs sequentially every day at 07:00 UTC',
    schedule_interval='0 7 * * *',  # daily at 07:00 UTC
    tags=['glue', 'daily', 'sequential']
) as dag:

    previous_task = None

    for job_name in GLUE_JOBS:
        task = PythonOperator(
            task_id=f'trigger_{job_name.replace("-", "_")}',
            python_callable=lambda job=job_name: trigger_glue_job(job)
        )

        if previous_task:
            previous_task >> task  # Sequential dependency
        previous_task = task
