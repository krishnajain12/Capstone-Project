from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import time

# AWS region
REGION = 'us-east-1'

# Daily Glue jobs
DAILY_GLUE_JOBS = [
    'poc-bootcamp-group-3-employee-data',
    'poc-bootcamp-group-3-employee-timeframe-data',
    'poc-bootcamp-group-3-employee-leave-data',
    'poc-bootcamp-group-3-count_by_designation',
    'poc-bootcamp-group-3-threshold'
]

# Monthly job
MONTHLY_JOB = 'poc-bootcamp-group-3-Quota_80%'

# Function to trigger and wait for Glue job to complete
def trigger_glue_job(job_name):
    client = boto3.client('glue', region_name=REGION)

    response = client.start_job_run(JobName=job_name)
    job_run_id = response['JobRunId']
    print(f"Started Glue job: {job_name}, Run ID: {job_run_id}")

    while True:
        run_state = client.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']['JobRunState']
        print(f"Job {job_name} current state: {run_state}")

        if run_state in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
            break
        time.sleep(30)

    if run_state != 'SUCCEEDED':
        raise Exception(f"Glue job {job_name} failed with state: {run_state}")
    print(f"Glue job {job_name} completed successfully.")

# Custom wrapper to skip the monthly job on other days
def run_monthly_quota_if_first_day():
    if datetime.utcnow().day == 1:
        trigger_glue_job(MONTHLY_JOB)
    else:
        print("Not the 1st of the month — skipping Quota_80% job.")

# Default task settings
default_args = {
    'owner': 'krishna',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# DAG definition
with DAG(
    dag_id='daily_employee_glue_jobs_sequential',
    default_args=default_args,
    description='Run 5 daily Glue jobs and 1 conditional monthly job (Quota_80%) in sequence at 07:00 UTC',
    schedule_interval='0 7 * * *',  # Daily at 07:00 UTC
    tags=['glue', 'daily', 'conditional']
) as dag:

    previous_task = None

    for job_name in DAILY_GLUE_JOBS:
        task = PythonOperator(
            task_id=f'trigger_{job_name.replace("-", "_")}',
            python_callable=lambda job=job_name: trigger_glue_job(job)
        )

        if previous_task:
            previous_task >> task  # enforce order
        previous_task = task

    # Quota_80% job: only runs if today is the 1st, and after leave-data
    quota_job = PythonOperator(
        task_id='trigger_quota_80_percent_if_first',
        python_callable=run_monthly_quota_if_first_day
    )

    # Attach it to run *after* leave-data
    trigger_leave_data = dag.get_task('trigger_poc_bootcamp_group_3_employee_leave_data')
    trigger_leave_data >> quota_job
