from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='hr_data_etl',
    default_args=default_args,
    description='Load HR CSV from GCS and transform it in BigQuery',
    schedule_interval='@daily',
    catchup=False,
)

# Task 1: Load CSV from GCS to BigQuery
load_hr_data = GCSToBigQueryOperator(
    task_id='load_hr_csv_to_bigquery',
    bucket='hr-data-bucket-1',
    source_objects=['HumanResources.csv'],
    destination_project_dataset_table='intricate-pad-455815-j1.hr_project.hr_cleaned',
    skip_leading_rows=1,
    source_format='CSV',
    autodetect=True,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

# Task 2: Transform Data in BigQuery (Optional)
transform_query = """
CREATE OR REPLACE TABLE `intricate-pad-455815-j1.hr_project.hr_final` AS
SELECT *, CURRENT_TIMESTAMP() AS last_updated
FROM `intricate-pad-455815-j1.hr_project.hr_cleaned`
"""

transform_data = BigQueryInsertJobOperator(
    task_id='transform_hr_data',
    configuration={
        "query": {
            "query": transform_query,
            "useLegacySql": False
        }
    },
    dag=dag,
)

load_hr_data >> transform_data
