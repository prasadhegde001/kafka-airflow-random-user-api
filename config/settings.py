from pathlib import Path

# Project root (works from any cwd, e.g. Airflow runs from /opt/airflow)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


## Uncomment when running localy

# KAFKA_BROKER = "localhost:9092"
# SCHEMA_REGISTRY_URL = "http://localhost:8081"


## Comment this when running localy
KAFKA_BROKER = "kafka:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

# Topic → Avro schema file (absolute paths so tasks work when cwd is not the repo root)
TOPIC_SCHEMA_MAP = {
    "random-users-info": str(PROJECT_ROOT / "schema/schema_registry/user_schema.avsc"),
}

# Topic Configs
TOPIC_CONFIG = {
    "num_partitions"       : 3,
    "replication_factor"   : 1,
    "retention_ms"         : "604800000",   # 7 days
    "cleanup_policy"       : "delete"
}
