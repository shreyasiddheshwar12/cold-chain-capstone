# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ### Incremental Cold-Chain Data Generation
# MAGIC # Generates one immutable, unique 500-row sensor batch per run.

# COMMAND ----------

# Configures the incremental data generator, storage paths,
# batch identity, run identity and reproducible random seed.

import hashlib
import json
import random
import uuid

from datetime import datetime, timezone


spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC"
)


# ---------------------------------------------------------
# Unity Catalog and storage configuration
# ---------------------------------------------------------

CATALOG = "training_lab_classic"
SCHEMA = "cold_chain"
VOLUME = "volume1"

BASE = (
    f"/Volumes/{CATALOG}/"
    f"{SCHEMA}/{VOLUME}"
)

PRODUCTS_PATH = (
    f"{BASE}/landing/product_limits"
)

SHIPMENTS_PATH = (
    f"{BASE}/landing/shipments"
)

SENSOR_STAGING = (
    f"{BASE}/staging/sensor_events"
)


# ---------------------------------------------------------
# Incremental batch configuration
# ---------------------------------------------------------

LOAD_MODE = "incremental"

TOTAL_SENSOR_RECORDS = 500
BATCH_SIZE = 500

BAD_RECORD_RATE = 0.01
EXCEPTION_RATE = 0.03
BOUNDARY_RATE = 0.01

SEED = 42


# ---------------------------------------------------------
# Optional parameters supplied by Databricks Job / Airflow
# ---------------------------------------------------------

dbutils.widgets.text(
    "run_id",
    ""
)

dbutils.widgets.text(
    "batch_id",
    ""
)

INPUT_RUN_ID = (
    dbutils.widgets.get("run_id")
    .strip()
)

INPUT_BATCH_ID = (
    dbutils.widgets.get("batch_id")
    .strip()
)



# ---------------------------------------------------------
# Batch ID
# ---------------------------------------------------------

# If no batch ID is supplied, generate a unique UTC value.

AUTO_BATCH_ID = datetime.now(
    timezone.utc
).strftime(
    "%Y%m%dT%H%M%S%fZ"
)

BATCH_ID = (
    INPUT_BATCH_ID
    if INPUT_BATCH_ID
    else AUTO_BATCH_ID
)

SAFE_BATCH_ID = "".join(
    character
    if (
        character.isalnum()
        or character in {"_", "-"}
    )
    else "_"
    for character in BATCH_ID
)

assert SAFE_BATCH_ID, (
    "batch_id contains no usable characters"
)


dbutils.widgets.text(
    "run_id",
    ""
)

RUN_ID_INPUT = (
    dbutils.widgets.get("run_id")
    .strip()
)

RUN_ID = (
    RUN_ID_INPUT
    if RUN_ID_INPUT
    else f"manual_{uuid.uuid4()}"
)

assert RUN_ID, (
    "Pipeline run_id could not be resolved"
)




# ---------------------------------------------------------
# Storage initialization
# ---------------------------------------------------------

dbutils.fs.mkdirs(
    SENSOR_STAGING
)


# ---------------------------------------------------------
# Deterministic random generator for this batch
# ---------------------------------------------------------

batch_seed = int(
    hashlib.sha256(
        SAFE_BATCH_ID.encode("utf-8")
    ).hexdigest()[:8],
    16
)

rng = random.Random(
    SEED + batch_seed
)


# ---------------------------------------------------------
# Configuration summary
# ---------------------------------------------------------

print("==============================================")
print("INCREMENTAL GENERATOR CONFIGURATION")
print("==============================================")

print("Run ID :", RUN_ID)
print("Batch ID        :", SAFE_BATCH_ID)
print("Load mode       :", LOAD_MODE)
print("Record count    :", TOTAL_SENSOR_RECORDS)
print("Batch size      :", BATCH_SIZE)
print("Bad-record rate :", BAD_RECORD_RATE)
print("Exception rate  :", EXCEPTION_RATE)
print("Boundary rate   :", BOUNDARY_RATE)
print("Staging path    :", SENSOR_STAGING)

# COMMAND ----------

# Read reference data initialized by the historical notebook.
products_df = spark.read.option("header", True).option("inferSchema", True).csv(PRODUCTS_PATH)
shipments_df = spark.read.option("header", True).csv(SHIPMENTS_PATH)

assert products_df.count() > 0, "Product reference data is missing"
assert shipments_df.count() > 0, "Shipment reference data is missing"

# COMMAND ----------

def parse_utc(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def format_utc(value):
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


limits = {
    row["product_id"]: (float(row["min_temperature"]), float(row["max_temperature"]))
    for row in products_df.collect()
}
shipment_rows = shipments_df.collect()

bad_record_count = round(TOTAL_SENSOR_RECORDS * BAD_RECORD_RATE)
good_record_count = TOTAL_SENSOR_RECORDS - bad_record_count
per_shipment, remainder = divmod(good_record_count, len(shipment_rows))

good_sensor_events = []
exception_count = 0
boundary_count = 0

for shipment_index, shipment in enumerate(shipment_rows):
    shipment_id = shipment["shipment_id"]
    sensor_id = "S" + shipment_id[2:]
    minimum_temperature, maximum_temperature = limits[shipment["product_id"]]
    dispatch_time = parse_utc(shipment["dispatch_time"])
    arrival_time = parse_utc(shipment["expected_arrival"])
    reading_count = per_shipment + (1 if shipment_index < remainder else 0)

    for _ in range(reading_count):
        event_time = datetime.fromtimestamp(
            rng.uniform(dispatch_time.timestamp(), arrival_time.timestamp()),
            tz=timezone.utc,
        )
        probability = rng.random()

        if probability < EXCEPTION_RATE:
            exception_count += 1
            temperature = (
                maximum_temperature + rng.uniform(0.5, 5.0)
                if rng.random() < 0.5
                else minimum_temperature - rng.uniform(0.5, 3.0)
            )
        elif probability < EXCEPTION_RATE + BOUNDARY_RATE:
            boundary_count += 1
            temperature = minimum_temperature if rng.random() < 0.5 else maximum_temperature
        else:
            temperature = rng.uniform(minimum_temperature, maximum_temperature)

        event_number = len(good_sensor_events) + 1
        good_sensor_events.append(
            (
                f"{SAFE_BATCH_ID}_E{event_number:06d}",
                shipment_id,
                sensor_id,
                format_utc(event_time),
                f"{temperature:.2f}",
            )
        )

bad_types = [
    "invalid_temperature",
    "duplicate_event_id",
    "missing_event_id",
    "unknown_shipment",
]
invalid_temperatures = ["hot", "N/A", "error", "--"]
bad_sensor_events = []
bad_start_time = parse_utc(shipment_rows[0]["dispatch_time"])

for bad_index in range(bad_record_count):
    bad_type = bad_types[bad_index % len(bad_types)]
    event_time_string = format_utc(bad_start_time)
    bad_event_id = f"{SAFE_BATCH_ID}_BAD{bad_index + 1:06d}"

    if bad_type == "invalid_temperature":
        record = (bad_event_id, "SH005", "S005", event_time_string, invalid_temperatures[bad_index % len(invalid_temperatures)])
    elif bad_type == "duplicate_event_id":
        record = good_sensor_events[bad_index % len(good_sensor_events)]
    elif bad_type == "missing_event_id":
        record = (None, "SH005", "S005", event_time_string, "5.0")
    else:
        record = (bad_event_id, "SH999", "S999", event_time_string, "5.0")

    bad_sensor_events.append(record)

sensor_events = good_sensor_events + bad_sensor_events
rng.shuffle(sensor_events)
assert len(sensor_events) == TOTAL_SENSOR_RECORDS

# COMMAND ----------

filename = f"{LOAD_MODE}_{SAFE_BATCH_ID}_001.json"
output_path = f"{SENSOR_STAGING}/{filename}"

# Immutable batch policy: replay of the same batch_id is skipped safely.
existing_names = {
    file_info.name for file_info in dbutils.fs.ls(SENSOR_STAGING)
}

if filename in existing_names:
    print("==============================================")
    print("INCREMENTAL DATA GENERATION: SKIPPED")
    print("==============================================")
    print("Batch already exists:", output_path)
    dbutils.notebook.exit(
        json.dumps(
            {
                "status": "SKIPPED",
                "batch_id": SAFE_BATCH_ID,
                "file": output_path,
                "rows_written": 0,
            }
        )
    )

json_lines = []
for row in sensor_events:
    json_lines.append(
        json.dumps(
            {
                "event_id": row[0],
                "shipment_id": row[1],
                "sensor_id": row[2],
                "event_time": row[3],
                "temperature_c": row[4],
                "batch_id": SAFE_BATCH_ID,
                "load_mode": LOAD_MODE,
            }
        )
    )

dbutils.fs.put(output_path, "\n".join(json_lines) + "\n", overwrite=False)

written_file = [
    file_info
    for file_info in dbutils.fs.ls(SENSOR_STAGING)
    if file_info.name == filename
]
assert len(written_file) == 1

print("==============================================")
print("INCREMENTAL DATA GENERATION: PASS")
print("==============================================")
print("Batch ID             :", SAFE_BATCH_ID)
print("Sensor records       :", len(sensor_events))
print("Good base records    :", len(good_sensor_events))
print("Controlled bad rows  :", len(bad_sensor_events))
print("Temperature exceptions:", exception_count)
print("Boundary readings    :", boundary_count)
print("Generated file       :", output_path)
display(written_file)