from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from dags.dw_etl.dw_gold_layer import DataLakeReader as agg_table
from dw_etl.dw_silver_layer import DataLakeTransformer as silver
from dw_etl.dw_bronze_layer import ExtractData as bronze


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('extract_api_data', default_args=default_args, schedule_interval='@daily')

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=bronze().extract_data,
    provide_context=True,
    dag=dag,
)

transform_silver_task = PythonOperator(
    task_id='transform_to_silver',
    python_callable=silver().transform_to_silver,
    provide_context=True,
    dag=dag,
)

transform_gold_task = PythonOperator(
    task_id='transform_to_gold',
    python_callable=agg_table().aggregate_breweries,
    provide_context=True,
    dag=dag,
)


extract_task >> transform_silver_task >> transform_gold_task