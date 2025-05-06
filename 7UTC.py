from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'krishna',
    'start_date': datetime(2025, 5, 1),
    'retries': 0,
    'catchup': False
}

DAILY_GLUE_JOBS = [
    'poc-bootcamp-group-3-employee-data',
    'poc-bootcamp-group-3-employee-timeframe-data',
    'poc-bootcamp-group-3-employee-leave-data',
    'poc-bootcamp-group-3-count_by_designation',
    'poc-bootcamp-group-3-threshold'
]

MONTHLY_JOB = 'poc-bootcamp-group-3-Quota_80%'

with DAG(
    dag_id='daily_employee_glue_jobs_sequential',
    default_args=default_args,
    schedule_interval='0 7 * * *',
    max_active_runs=1,
    description='Trigger Glue jobs in sequence with optional monthly job',
    tags=['glue', 'daily', 'conditional']
) as dag:

    previous_task = None

    for job_name in DAILY_GLUE_JOBS:
        task = GlueJobOperator(
            task_id=f'trigger_{job_name.replace("-", "_")}',
            job_name=job_name,
            region_name='us-east-1',
            wait_for_completion=True
        )

        if previous_task:
            previous_task >> task
        previous_task = task

    # Conditional Monthly Job (runs only on 1st)
    from airflow.operators.python import BranchPythonOperator
    from airflow.operators.dummy import DummyOperator

    def is_first_day():
        return 'quota_job' if datetime.utcnow().day == 1 else 'skip_quota'

    check_day = BranchPythonOperator(
        task_id='check_first_day',
        python_callable=is_first_day
    )

    quota_job = GlueJobOperator(
        task_id='quota_job',
        job_name=MONTHLY_JOB,
        region_name='us-east-1',
        wait_for_completion=True
    )

    skip_quota = DummyOperator(task_id='skip_quota')

    previous_task >> check_day
    check_day >> [quota_job, skip_quota]
