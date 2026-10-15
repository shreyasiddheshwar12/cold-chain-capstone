from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)

with DAG(
    dag_id="cold_chain_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["cold-chain", "databricks"],
) as dag:

    run_cold_chain_pipeline = DatabricksRunNowOperator(
        task_id="run_cold_chain_pipeline",
        databricks_conn_id="databricks_default",
        job_id=522247114408497,
        wait_for_termination=True,
        retries=0,
    )
