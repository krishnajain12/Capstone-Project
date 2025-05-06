from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

# AWS region and updated Glue job names
REGION = 'us-east-1'
GLUE_JOBS = [
    'poc-bootcamp-group-3-employee-leave-quota',
    'poc-bootcamp-group3-leave-calender-data'
]

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

    trigger_quota_job = GlueJobOperator(
        task_id='trigger_employee_leave_quota',
        job_name='poc-bootcamp-group-3-employee-leave-quota',
        region_name=REGION,
        wait_for_completion=True
    )

    trigger_calendar_job = GlueJobOperator(
        task_id='trigger_employee_leave_calender_data',
        job_name='poc-bootcamp-group3-leave-calender-data',
        region_name=REGION,
        wait_for_completion=True
    )

    # No dependencies = both run in parallel
