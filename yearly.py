from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3

# AWS region and updated Glue job names
REGION = 'us-east-1'
GLUE_JOBS = [
    'poc-bootcamp-group-3-employee-leave-quota',
    'poc-bootcamp-group3-leave-calender-data'
]

def trigger_glue_job(job_name):
    client = boto3.client('glue', region_name=REGION)
    response = client.start_job_run(JobName=job_name)
    print(f"Triggered Glue job: {job_name}, Run ID: {response['JobRunId']}")

default_args = {
    'owner': 'krishna',
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='annual_employee_glue_jobs_parallel',
    default_args=default_args,
    description='Triggers both AWS Glue jobs in parallel every Jan 1st',
    schedule_interval='@yearly',
    tags=['glue', 'employee', 'parallel']
) as dag:

    trigger_quota_job = PythonOperator(
        task_id='trigger_employee_leave_quota',
        python_callable=lambda: trigger_glue_job('poc-bootcamp-group-3-employee-leave-quota')
    )

    trigger_calendar_job = PythonOperator(
        task_id='trigger_employee_leave_calender_data',
        python_callable=lambda: trigger_glue_job('poc-bootcamp-group3-leave-calender-data')
    )

    # No dependency → runs in parallel
