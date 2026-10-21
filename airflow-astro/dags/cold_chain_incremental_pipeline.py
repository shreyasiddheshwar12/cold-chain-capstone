import json
import os
from datetime import datetime, timezone

import requests

from airflow.sdk import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)


# ---------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------

DATABRICKS_JOB_ID_RAW = os.getenv(
    "DATABRICKS_INCREMENTAL_JOB_ID",
    "",
).strip()

NEW_RELIC_LICENSE_KEY = os.getenv(
    "NEW_RELIC_LICENSE_KEY",
    "",
).strip()

NEW_RELIC_REGION = os.getenv(
    "NEW_RELIC_REGION",
    "US",
).strip().upper()


if not DATABRICKS_JOB_ID_RAW:
    raise ValueError(
        "DATABRICKS_INCREMENTAL_JOB_ID is missing."
    )


DATABRICKS_JOB_ID = int(
    DATABRICKS_JOB_ID_RAW
)


NEW_RELIC_ENDPOINTS = {
    "US": (
        "https://insights-collector.newrelic.com"
        "/v1/accounts/events"
    ),
    "EU": (
        "https://insights-collector.eu01.nr-data.net"
        "/v1/accounts/events"
    ),
    "JP": (
        "https://insights-collector.jp.nr-data.net"
        "/v1/accounts/events"
    ),
}


if NEW_RELIC_REGION not in NEW_RELIC_ENDPOINTS:
    raise ValueError(
        "NEW_RELIC_REGION must be US, EU, or JP."
    )


# ---------------------------------------------------------
# Airflow-level New Relic telemetry
# ---------------------------------------------------------

def publish_airflow_event(
    context,
    status,
):
    if not NEW_RELIC_LICENSE_KEY:
        print(
            "NEW_RELIC_LICENSE_KEY is unavailable. "
            "Skipping Airflow telemetry."
        )
        return

    dag_run = context.get(
        "dag_run"
    )

    task_instance = context.get(
        "task_instance"
    )

    exception = context.get(
        "exception"
    )

    logical_date = context.get(
        "logical_date"
    )

    event = {
        "eventType": "ColdChainAirflowRun",
        "team": "cold-chain-capstone",
        "pipeline": (
            "cold_chain_incremental_pipeline"
        ),
        "environment": "training",
        "status": status,
        "airflowDagId": (
            context["dag"].dag_id
        ),
        "airflowRunId": (
            dag_run.run_id
            if dag_run
            else None
        ),
        "airflowTaskId": (
            task_instance.task_id
            if task_instance
            else None
        ),
        "logicalDate": (
            logical_date.isoformat()
            if logical_date
            else None
        ),
        "errorMessage": (
            str(exception)[:1000]
            if exception
            else None
        ),
        "observedAt": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    event = {
        key: value
        for key, value in event.items()
        if value is not None
    }

    response = requests.post(
        NEW_RELIC_ENDPOINTS[
            NEW_RELIC_REGION
        ],
        headers={
            "Api-Key": (
                NEW_RELIC_LICENSE_KEY
            ),
            "Content-Type": (
                "application/json"
            ),
        },
        data=json.dumps(
            [event]
        ),
        timeout=30,
    )

    response.raise_for_status()

    print(
        "Airflow telemetry published:",
        status,
        event.get("airflowRunId"),
    )


def notify_success(context):
    publish_airflow_event(
        context=context,
        status="SUCCESS",
    )


def notify_failure(context):
    publish_airflow_event(
        context=context,
        status="FAILED",
    )


# ---------------------------------------------------------
# Airflow DAG
# ---------------------------------------------------------

with DAG(
    dag_id=(
        "cold_chain_incremental_pipeline"
    ),
    description=(
        "Triggers the complete incremental "
        "cold-chain Databricks workflow"
    ),
    start_date=datetime(
        2026,
        10,
        21,
        tzinfo=timezone.utc,
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "team-2",
        "retries": 0,
    },
    tags=[
        "cold-chain",
        "incremental",
        "databricks",
        "new-relic",
    ],
) as dag:

    run_incremental_databricks_job = (
        DatabricksRunNowOperator(
            task_id=(
                "run_incremental_databricks_job"
            ),
            databricks_conn_id=(
                "databricks_default"
            ),
            job_id=DATABRICKS_JOB_ID,
            wait_for_termination=True,
            polling_period_seconds=30,
            retries=0,
            on_success_callback=(
                notify_success
            ),
            on_failure_callback=(
                notify_failure
            ),
        )
    )
