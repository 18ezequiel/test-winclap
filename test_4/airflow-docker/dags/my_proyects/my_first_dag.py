from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from my_proyects.analytics_pipeline.extractor_loader import AnalyticsExtractorAndLoader
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, OrderBy
import os
from google.cloud import bigquery

# Adjust the start_date to be a date in the past
start_date = datetime(2023, 8, 17)  # One week before August 24, 2023

# Define the DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance
dag = DAG(
    'analytics_pipeline_dag',
    default_args=default_args,
    schedule_interval=timedelta(weeks=1),  # Will run every week
    catchup=False,
)

def run_analytics_pipeline():
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/opt/airflow/ga-analytics-397004-c6f5acdcbd7a.json'
    extractor_loader = AnalyticsExtractorAndLoader(os.environ["GOOGLE_APPLICATION_CREDENTIALS"], '404209256')

    dimensions = [
        Dimension(name="week"), 
        Dimension(name="sessionMedium"),
        Dimension(name="country")
    ]
    metrics = [
        Metric(name="averageSessionDuration"), 
        Metric(name="activeUsers")
    ]
    order_bys = [
        OrderBy(dimension={'dimension_name': 'week'}),
        OrderBy(dimension={'dimension_name': 'sessionMedium'})
    ]
    date_range = DateRange(start_date="2023-08-18", end_date="today")
    data_df = extractor_loader.extract_data(dimensions, metrics, order_bys, date_range)
    table_id = 'ga-analytics-397004.ga_analytics_397004_ezequiel.table_testing_1'
    # Agregar la columna de fecha de inicio de la semana
    data_df['week_start_date'] = date_range.start_date

    # Resetear los índices y mantener solo las columnas necesarias
    data_df_reset = data_df.reset_index()
    data_df_for_bigquery = data_df_reset[['week', 'sessionMedium', 'country', 'averageSessionDuration', 'activeUsers', 'week_start_date']]
    print(data_df)
    # Cargar data_df_for_bigquery en BigQuery
    rows_loaded, columns_loaded = extractor_loader.load_to_bigquery(data_df_for_bigquery, table_id)

# Define the PythonOperator that will execute the code
run_pipeline_task = PythonOperator(
    task_id='run_pipeline_task',
    python_callable=run_analytics_pipeline,
    dag=dag,
)



run_pipeline_task
