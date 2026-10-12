from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="cold_chain_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    generate_data = EmptyOperator(task_id="generate_data")

    bronze_load = EmptyOperator(task_id="bronze_load")

    silver_transform = EmptyOperator(task_id="silver_transform")

    gold_aggregation = EmptyOperator(task_id="gold_aggregation")

    generate_data >> bronze_load >> silver_transform >> gold_aggregation
