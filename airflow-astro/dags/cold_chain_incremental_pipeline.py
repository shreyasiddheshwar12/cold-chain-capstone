import json
import os
import time
from datetime import datetime, timezone

import requests
from airflow.sdk import DAG, task


DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_INCREMENTAL_JOB_ID", "")

NEW_RELIC_LICENSE_KEY = os.getenv("NEW_RELIC_LICENSE_KEY", "")
NEW_RELIC_REGION = os.getenv("NEW_RELIC_REGION", "US").upper()

NEW_RELIC_ENDPOINTS = {
    "US": "https://insights-collector.newrelic.com/v1/accounts/events",
    "EU": "https://insights-collector.eu01.nr-data.net/v1/accounts/events",
    "JP": "https://insights-collector.jp.nr-data.net/v1/accounts/events",
}


def publish_airflow_event(context, status):
    if not NEW_RELIC_LICENSE_KEY:
        print("New Relic key missing. Skipping Airflow telemetry.")
        return

    dag_run = context.get("dag_run")
    exception = context.get("exception")

    event = {
        "eventType": "ColdChainAirflowRun",
        "team": "cold-chain-capstone",
        "pipeline": "cold_chain_incremental_pipeline",
        "environment": "training",
        "status": status,
        "airflowDagId": context["dag"].dag_id,
        "airflowRunId": dag_run.run_id if dag_run else None,
        "errorMessage": str(exception)[:1000] if exception else None,
        "observedAt": datetime.now(timezone.utc).isoformat(),
    }
    event = {k: v for k, v in event.items() if v is not None}

    response = requests.post(
        NEW_RELIC_ENDPOINTS.get(NEW_RELIC_REGION, NEW_RELIC_ENDPOINTS["US"]),
        headers={
            "Api-Key": NEW_RELIC_LICENSE_KEY,
            "Content-Type": "application/json",
        },
        data=json.dumps([event]),
        timeout=30,
    )
    response.raise_for_status()
    print("Airflow telemetry published:", status)


def notify_success(context):
    publish_airflow_event(context, "SUCCESS")


def notify_failure(context):
    publish_airflow_event(context, "FAILED")


with DAG(
    dag_id="cold_chain_incremental_pipeline",
    description="Triggers the incremental cold-chain Databricks Job",
    start_date=datetime(2026, 10, 21, tzinfo=timezone.utc),
    schedule="0 */12 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "team-2", "retries": 0},
    tags=["cold-chain", "incremental", "databricks", "new-relic"],
) as dag:

    @task(
        on_success_callback=notify_success,
        on_failure_callback=notify_failure,
    )
    def run_incremental_databricks_job():
        assert DATABRICKS_HOST, "DATABRICKS_HOST is missing"
        assert DATABRICKS_TOKEN, "DATABRICKS_TOKEN is missing"
        assert DATABRICKS_JOB_ID, "DATABRICKS_INCREMENTAL_JOB_ID is missing"

        headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

        start = requests.post(
            f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
            headers=headers,
            json={"job_id": int(DATABRICKS_JOB_ID)},
            timeout=60,
        )
        start.raise_for_status()
        run_id = start.json()["run_id"]
        print("Databricks run started:", run_id)

        while True:
            time.sleep(30)

            status = requests.get(
                f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
                headers=headers,
                params={"run_id": run_id},
                timeout=60,
            )
            status.raise_for_status()
            state = status.json()["state"]

            life_cycle = state.get("life_cycle_state")
            result = state.get("result_state")
            print("Life cycle:", life_cycle, "| Result:", result)

            if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                if result != "SUCCESS":
                    raise RuntimeError(
                        f"Databricks run {run_id} ended with {result}: "
                        f"{state.get('state_message', '')}"
                    )
                print("Databricks run succeeded:", run_id)
                return run_id

    run_incremental_databricks_job()