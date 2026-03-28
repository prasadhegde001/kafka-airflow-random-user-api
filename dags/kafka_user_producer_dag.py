import uuid
import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Default DAG arguments
# ─────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    'start_date': datetime(2023, 9, 3, 10, 00)
}

# ─────────────────────────────────────────────────────────────────────────────
# Task 1 — Fetch & format a random user from the API
#           Pushes the result to XCom so downstream tasks can consume it
# ─────────────────────────────────────────────────────────────────────────────

def fetch_and_format_user(**context):

    try:
        response = requests.get("https://randomuser.me/api/", timeout=10)
        response.raise_for_status()
        result = response.json()["results"][0]
    except requests.exceptions.ConnectionError:
        raise RuntimeError("Failed to connect to randomuser.me. Check network.")
    except requests.exceptions.Timeout:
        raise RuntimeError("Request to randomuser.me timed out.")
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"HTTP Error: {e.response.status_code} - {e.response.reason}")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Unexpected requests error: {e}")
    except (ValueError, KeyError) as e:
        raise RuntimeError(f"Failed to parse API response: {e}")

    location = result["location"]

    user_data = {
        "id":              str(uuid.uuid4()),
        "first_name":      result["name"]["first"],
        "last_name":       result["name"]["last"],
        "gender":          result["gender"],
        "address":         (
            f"{location['street']['number']} {location['street']['name']}, "
            f"{location['city']}, {location['state']}, {location['country']}"
        ),
        "post_code":       str(location["postcode"]),
        "email":           result["email"],
        "username":        result["login"]["username"],
        "dob":             result["dob"]["date"],
        "registered_date": result["registered"]["date"],
        "phone":           result["phone"],
        "picture":         result["picture"]["medium"],
    }

    log.info("Fetched user: %s %s (id=%s)", user_data["first_name"], user_data["last_name"], user_data["id"])

    # Return value is automatically pushed to XCom under key "return_value"
    return user_data

# ─────────────────────────────────────────────────────────────────────────────
# Task 2 — Register all Avro schemas with the Schema Registry
# ─────────────────────────────────────────────────────────────────────────────

def register_schemas(**context):

    from schema.register_client.registry_client import register_all_schemas

    log.info("Registering Avro schemas with Schema Registry...")

    register_all_schemas()

    log.info("Schema registration complete.")


# ─────────────────────────────────────────────────────────────────────────────
# Task 3 — Produce the formatted user record to Kafka
#           Pulls the user dict from XCom (pushed by Task 1)
# ─────────────────────────────────────────────────────────────────────────────

def produce_to_kafka(**context):

    from producer.avro_producer import produce_records

    # Pull the user dict that Task 1 pushed to XCom
    user_data = context["ti"].xcom_pull(task_ids="fetch_and_format_user")

    if not user_data:
        raise ValueError("No user data received from XCom. Upstream task may have failed.")

    topic = "random-users-info"

    log.info("Producing record for user id=%s to topic '%s'", user_data["id"], topic)

    report = produce_records(
        topic        = topic,
        records      = [user_data],
        key_field    = "id",
        skip_invalid = True,
    )

    log.info("Produce report: %s", report)

    # Push report to XCom for observability (optional)
    return report


# ─────────────────────────────────────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id          = "kafka_random_user_producer",
    description     = "Fetch a random user from the API, register Avro schema, and produce to Kafka",
    default_args    = default_args,
    start_date      = datetime(2026, 3, 27),
    schedule_interval = "@hourly",   
    catchup         = False,         
    max_active_runs = 1,             
    tags            = ["kafka", "avro", "random-user"],
) as dag:

    t1_fetch_user = PythonOperator(
        task_id          = "fetch_and_format_user",
        python_callable  = fetch_and_format_user,
    )

    t2_register_schema = PythonOperator(
        task_id          = "register_schemas",
        python_callable  = register_schemas,
    )

    t3_produce = PythonOperator(
        task_id          = "produce_to_kafka",
        python_callable  = produce_to_kafka,
    )

    
    t1_fetch_user >> t2_register_schema >> t3_produce
