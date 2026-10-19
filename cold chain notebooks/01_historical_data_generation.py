# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Historical Cold-Chain Data Generation
# MAGIC Generates reference data and one immutable 4,000-row historical sensor load.

# COMMAND ----------

import json
import random
from datetime import datetime, timezone

spark.conf.set("spark.sql.session.timeZone", "UTC")

SEED = 42
rng = random.Random(SEED)

CATALOG = "training_lab_classic"
SCHEMA = "cold_chain"
VOLUME = "volume1"

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
SHIPMENTS_PATH = f"{BASE}/landing/shipments"
PRODUCTS_PATH = f"{BASE}/landing/product_limits"
DELIVERIES_PATH = f"{BASE}/landing/delivery_events"
SENSOR_STAGING = f"{BASE}/staging/sensor_events"

LOAD_MODE = "historical"
BATCH_ID = "historical_001"
TOTAL_SENSOR_RECORDS = 10000
BATCH_SIZE = 1000
BAD_RECORD_RATE = 0.01
EXCEPTION_RATE = 0.03
BOUNDARY_RATE = 0.01

for path in [SHIPMENTS_PATH, PRODUCTS_PATH, DELIVERIES_PATH, SENSOR_STAGING]:
    dbutils.fs.mkdirs(path)

print("Historical generator initialized")
print("Base path   :", BASE)
print("Batch ID    :", BATCH_ID)
print("Record count:", TOTAL_SENSOR_RECORDS)

# COMMAND ----------

products = [
    ("P001", 2.0, 8.0),
    ("P002", 3.0, 7.0),
    ("P003", 1.0, 5.0),
]

products_df = spark.createDataFrame(
    products,
    "product_id STRING, min_temperature DOUBLE, max_temperature DOUBLE",
)

shipments = [
    ("SH001", "P001", "V001", "Pune", "Mumbai", "2026-10-07T08:00:00Z", "2026-10-07T12:00:00Z"),
    ("SH002", "P002", "V002", "Mumbai", "Pune", "2026-10-07T08:30:00Z", "2026-10-07T13:00:00Z"),
    ("SH003", "P003", "V003", "Pune", "Bengaluru", "2026-10-07T07:00:00Z", "2026-10-07T15:00:00Z"),
    ("SH004", "P001", "V004", "Delhi", "Pune", "2026-10-07T06:00:00Z", "2026-10-07T14:00:00Z"),
    ("SH005", "P002", "V005", "Hyderabad", "Chennai", "2026-10-07T09:00:00Z", "2026-10-07T16:00:00Z"),
]

shipments_df = spark.createDataFrame(
    shipments,
    """
    shipment_id STRING, product_id STRING, vehicle_id STRING,
    origin STRING, destination STRING, dispatch_time STRING,
    expected_arrival STRING
    """,
)

delivery_events = [
    ("D001", "SH001", "DELIVERED", "2026-10-07T12:30:00Z"),
    ("D002", "SH002", "DELIVERED", "2026-10-07T12:50:00Z"),
    ("D004", "SH004", "DELIVERED", "2026-10-07T14:00:00Z"),
    ("D005", "SH005", "DELIVERED", "2026-10-07T17:00:00Z"),
]

deliveries_df = spark.createDataFrame(
    delivery_events,
    "event_id STRING, shipment_id STRING, status STRING, event_time STRING",
)

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
                f"{BATCH_ID}_E{event_number:06d}",
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
bad_start_time = parse_utc(shipments[0][5])

for bad_index in range(bad_record_count):
    bad_type = bad_types[bad_index % len(bad_types)]
    event_time_string = format_utc(bad_start_time)
    bad_event_id = f"{BATCH_ID}_BAD{bad_index + 1:06d}"

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

print("Good records        :", len(good_sensor_events))
print("Controlled bad rows :", len(bad_sensor_events))
print("Temp exceptions     :", exception_count)
print("Boundary readings   :", boundary_count)

# COMMAND ----------

def write_sensor_batch(
    filename,
    records
):
    output_path = (
        f"{SENSOR_STAGING}/"
        f"{filename}"
    )

    json_lines = []

    for row in records:
        json_lines.append(
            json.dumps(
                {
                    "event_id": row[0],
                    "shipment_id": row[1],
                    "sensor_id": row[2],
                    "event_time": row[3],
                    "temperature_c": row[4],
                    "batch_id": BATCH_ID,
                    "load_mode": LOAD_MODE,
                }
            )
        )

    dbutils.fs.put(
        output_path,
        "\n".join(json_lines) + "\n",
        overwrite=True
    )

    return output_path


sensor_batches = [
    sensor_events[
        index:index + BATCH_SIZE
    ]
    for index in range(
        0,
        len(sensor_events),
        BATCH_SIZE
    )
]


expected_file_count = (
    TOTAL_SENSOR_RECORDS
    + BATCH_SIZE
    - 1
) // BATCH_SIZE


generated_paths = []

for batch_number, batch in enumerate(
    sensor_batches,
    start=1
):
    filename = (
        f"{LOAD_MODE}_"
        f"{BATCH_ID}_"
        f"{batch_number:03d}.json"
    )

    generated_path = write_sensor_batch(
        filename,
        batch
    )

    generated_paths.append(
        generated_path
    )

    print(
        f"Created/overwritten {filename}: "
        f"{len(batch)} rows"
    )


actual_generated_rows = sum(
    len(batch)
    for batch in sensor_batches
)


assert len(generated_paths) == expected_file_count, (
    "Historical file count mismatch: "
    f"expected={expected_file_count}, "
    f"actual={len(generated_paths)}"
)

assert actual_generated_rows == TOTAL_SENSOR_RECORDS, (
    "Historical row count mismatch: "
    f"expected={TOTAL_SENSOR_RECORDS}, "
    f"actual={actual_generated_rows}"
)


print("==============================================")
print("HISTORICAL DATA GENERATION: PASS")
print("==============================================")

print(
    "Generated files:",
    len(generated_paths)
)

print(
    "Generated rows :",
    actual_generated_rows
)

print(
    "Batch ID       :",
    BATCH_ID
)

# COMMAND ----------

# Reference data is initialized during the historical load.
products_df.write.mode("overwrite").option("header", True).csv(PRODUCTS_PATH)
shipments_df.write.mode("overwrite").option("header", True).csv(SHIPMENTS_PATH)
deliveries_df.write.mode("overwrite").json(DELIVERIES_PATH)

print("==============================================")
print("HISTORICAL DATA GENERATION: PASS")
print("==============================================")
print("Batch ID          :", BATCH_ID)
print("Sensor rows       :", len(sensor_events))
print("Sensor files      :", len(generated_paths))
print("Products          :", products_df.count())
print("Shipments         :", shipments_df.count())
print("Delivery events   :", deliveries_df.count())
print("Sensor staging    :", SENSOR_STAGING)
display(dbutils.fs.ls(SENSOR_STAGING))