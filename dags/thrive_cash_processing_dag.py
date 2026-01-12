from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import logging

# Add scripts to path so we can import functions
sys.path.append('/opt/airflow/scripts')
from ingest import download_data, validate_source
from fifo_logic import run_fifo_matching

# [cite_start]--- Alerting Logic (Requirement: Alert on failures [cite: 41]) ---
def notify_failure(context):
    task_id = context['task_instance'].task_id
    logging.error(f"🚨 ALARM: Task '{task_id}' Failed! Paging Data Engineering Team...")

default_args = {
    'owner': 'thrive_candidate',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=30),
    'on_failure_callback': notify_failure
}

@dag(
    dag_id='thrive_cash_processing_final',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['assessment', 'production']
)
def thrive_pipeline():

    # 1. DOWNLOAD DATA
    @task(task_id='download_data')
    def step_download():
        download_data()

    # 2. VALIDATE SOURCE
    @task(task_id='validate_source')
    def step_validate_source():
        validate_source()

    # 2.5 PREPARE STAGING (Required for FIFO script to read clean data)
    step_dbt_staging = BashOperator(
        task_id='prepare_staging_data',
        bash_command='cd /opt/airflow/dbt_project && dbt run --select staging'
    )

    # 3. PERFORM FIFO MATCHING
    @task(task_id='perform_fifo_matching')
    def step_fifo_matching():
        run_fifo_matching()

    # 3.5 BUILD INTERMEDIATE MODELS (Required: Materialize int_fifo_matched from Python output)
    step_build_intermediate = BashOperator(
        task_id='build_intermediate_models',
        bash_command='cd /opt/airflow/dbt_project && dbt run --select intermediate'
    )

    # 4. VALIDATE RESULTS (Requirement: Verify matching correctness [cite: 35])
    step_validate_results = BashOperator(
        task_id='validate_results',
        bash_command='cd /opt/airflow/dbt_project && dbt test --select int_fifo_matched'
    )

    # 5. BUILD ANALYTICS
    step_build_analytics = BashOperator(
        task_id='build_analytics',
        bash_command='cd /opt/airflow/dbt_project && dbt run --select marts'
    )

    # 6. SEND ALERTS (Success Notification)
    @task(task_id='send_alerts')
    def step_send_alerts():
        logging.info("✅ SUCCESS: Pipeline completed. FIFO Matching Verified. Reports Ready.")

    # --- Strict Flow Definition ---
    (
        step_download() 
        >> step_validate_source() 
        >> step_dbt_staging 
        >> step_fifo_matching() 
        >> step_build_intermediate
        >> step_validate_results 
        >> step_build_analytics 
        >> step_send_alerts()
    )

thrive_pipeline()