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

# Airflow task name : exact Databricks task key
# Replace values marked REPLACE with your actual Databricks task keys.
DATABRICKS_TASK_KEYS = {
    "generate_data": "REPLACE_GENERATOR_TASK_KEY",
    "bronze": "Bronze",
    "silver_validation": "Silver_validation",
    "silver_reference": "Silver_Reference",
    "gold": "Gold",
    "operational_readiness": "REPLACE_OPERATIONAL_TASK_KEY",
    "new_relic": "REPLACE_NEW_RELIC_TASK_KEY",
}

POLL_SECONDS = 20
TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def databricks_headers():
    assert DATABRICKS_HOST, "DATABRICKS_HOST is missing"
    assert DATABRICKS_TOKEN, "DATABRICKS_TOKEN is missing"
    return {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}


def get_run(run_id):
    response = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
        headers=databricks_headers(),
        params={"run_id": run_id},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def wait_for_databricks_task(run_id, task_key):
    """Waits for one task inside the Databricks Job run."""
    while True:
        run = get_run(run_id)

        matches = [
            t for t in run.get("tasks", [])
            if t.get("task_key") == task_key
        ]
        assert matches, (
            f"Task key '{task_key}' not found in Databricks run {run_id}. "
            "Check DATABRICKS_TASK_KEYS."
        )

        state = matches[-1].get("state", {})
        life_cycle = state.get("life_cycle_state")
        result = state.get("result_state")

        print(f"{task_key}: life_cycle={life_cycle}, result={result}")

        if life_cycle in TERMINAL_STATES:
            if result != "SUCCESS":
                raise RuntimeError(
                    f"Databricks task {task_key} ended with {result}: "
                    f"{state.get('state_message', '')}"
                )
            return task_key

        if run.get("state", {}).get("life_cycle_state") in TERMINAL_STATES and life_cycle is None:
            raise RuntimeError(f"Databricks task {task_key} never started.")

        time.sleep(POLL_SECONDS)


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
    description="Triggers and tracks each stage of the incremental Databricks Job",
    start_date=datetime(2026, 10, 21, tzinfo=timezone.utc),
    schedule="0 */12 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "team-2", "retries": 0},
    tags=["cold-chain", "incremental", "databricks", "new-relic"],
) as dag:

    @task
    def trigger_databricks_job():
        assert DATABRICKS_JOB_ID, "DATABRICKS_INCREMENTAL_JOB_ID is missing"

        response = requests.post(
            f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
            headers=databricks_headers(),
            json={"job_id": int(DATABRICKS_JOB_ID)},
            timeout=60,
        )
        response.raise_for_status()
        run_id = response.json()["run_id"]
        print("Databricks run started:", run_id)
        return run_id

    def make_stage_task(airflow_task_id, databricks_task_key, trigger_rule="all_success"):
        @task(task_id=airflow_task_id, trigger_rule=trigger_rule)
        def stage(run_id):
            return wait_for_databricks_task(run_id, databricks_task_key)
        return stage

    @task(
        trigger_rule="all_done",
        on_success_callback=notify_success,
        on_failure_callback=notify_failure,
    )
    def finalize_pipeline(run_id):
        """Waits for the whole Databricks run and reports the final result."""
        while True:
            state = get_run(run_id).get("state", {})
            life_cycle = state.get("life_cycle_state")
            result = state.get("result_state")
            print("Job:", life_cycle, result)

            if life_cycle in TERMINAL_STATES:
                if result != "SUCCESS":
                    raise RuntimeError(
                        f"Databricks run {run_id} ended with {result}: "
                        f"{state.get('state_message', '')}"
                    )
                return run_id

            time.sleep(POLL_SECONDS)

    run_id = trigger_databricks_job()

    generate = make_stage_task("generate_data", DATABRICKS_TASK_KEYS["generate_data"])(run_id)
    bronze = make_stage_task("bronze", DATABRICKS_TASK_KEYS["bronze"])(run_id)
    silver_val = make_stage_task("silver_validation", DATABRICKS_TASK_KEYS["silver_validation"])(run_id)
    silver_ref = make_stage_task("silver_reference", DATABRICKS_TASK_KEYS["silver_reference"])(run_id)
    gold = make_stage_task("gold", DATABRICKS_TASK_KEYS["gold"])(run_id)
    operational = make_stage_task("operational_readiness", DATABRICKS_TASK_KEYS["operational_readiness"])(run_id)

    # New Relic runs in Databricks with "All done", so track it even if earlier stages fail.
    new_relic = make_stage_task(
        "new_relic",
        DATABRICKS_TASK_KEYS["new_relic"],
        trigger_rule="all_done",
    )(run_id)

    final = finalize_pipeline(run_id)

    generate >> bronze >> silver_val >> silver_ref >> gold >> operational >> new_relic >> final
