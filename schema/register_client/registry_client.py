
import json
import logging
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError
from config.settings import SCHEMA_REGISTRY_URL, TOPIC_SCHEMA_MAP
import os

# ── Logger Setup ──────────────────────────────────────────────
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ── Singleton Registry Client ─────────────────────────────────
_registry_client = None

def get_registry_client() -> SchemaRegistryClient:
    """Create and return a reusable Schema Registry client."""
    global _registry_client
    if _registry_client is None:
        _registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    return _registry_client


def load_schema(schema_path: str) -> str:
    """Load Avro schema from .avsc file and return as JSON string."""
    with open(schema_path, "r") as f:
        return json.dumps(json.load(f))


def get_existing_schema(subject: str) -> dict | None:
    """
    Fetch the latest registered schema for a subject.
    Returns dict with schema_id, version, schema_str — or None if not found.
    """
    client = get_registry_client()
    try:
        registered = client.get_latest_version(subject)
        return {
            "schema_id"  : registered.schema_id,
            "version"    : registered.version,
            "schema_str" : registered.schema.schema_str,
        }
    except SchemaRegistryError as e:
        if e.error_code == 40401:   # Subject not found
            return None
        logger.error(f" Schema Registry error for [{subject}]: {e}")
        raise


def is_schema_changed(subject: str, new_schema_str: str) -> bool:
    """
    Compare local schema with the latest registered schema.
    Returns True if schema has changed or doesn't exist yet.
    """
    existing = get_existing_schema(subject)

    if existing is None:
        return True     # Not registered yet → needs registration

    existing_schema = json.loads(existing["schema_str"])
    new_schema      = json.loads(new_schema_str)

    return existing_schema != new_schema


def check_compatibility(subject: str, schema_str: str) -> bool:
    """
    Check if the new schema is compatible with the existing registered schema.
    Returns True if compatible.
    """
    client     = get_registry_client()
    new_schema = Schema(schema_str, schema_type="AVRO")
    try:
        return client.test_compatibility(subject, new_schema)
    except SchemaRegistryError as e:
        logger.warning(f"  Compatibility check failed for [{subject}]: {e}")
        return False


def register_schema(subject: str, schema_str: str) -> int:
    """
    Register a new Avro schema under a subject in Schema Registry.
    Returns the Schema ID assigned by the registry.
    """
    client     = get_registry_client()
    new_schema = Schema(schema_str, schema_type="AVRO")
    schema_id  = client.register_schema(subject, new_schema)
    logger.info(f"  ✅ Registered  → Subject: [{subject}] | Schema ID: {schema_id}")
    return schema_id


def register_all_schemas() -> dict:
    """
    Auto-register all topic schemas from TOPIC_SCHEMA_MAP.
    For each topic:
      1. Load local .avsc schema file
      2. Check if schema already exists in registry
      3. If exists & unchanged    → Skip
      4. If exists & changed      → Compatibility check → Register new version
      5. If not exists            → Register fresh schema
    Returns: {topic_name: schema_id}
    """
    logger.info("=" * 60)
    logger.info("   Kafka Schema Registry — Schema Registration")
    logger.info("=" * 60)

    registered = {}
    skipped    = 0
    failed     = 0

    for topic, schema_path in TOPIC_SCHEMA_MAP.items():
        subject = f"{topic}-value"   # Confluent naming convention

        logger.info(f"\n  Topic   : {topic}")
        logger.info(f"     Subject : {subject}")
        logger.info(f"     File    : {schema_path}")

        try:
           

            # ── Step 1: Load local schema ──────────────────────────
            new_schema_str = load_schema(schema_path)

            # ── Step 2: Check existing schema in registry ──────────
            existing = get_existing_schema(subject)

            if existing:
                logger.info(f"     📌 Existing schema found | Version: {existing['version']} | ID: {existing['schema_id']}")

                # ── Step 3: Compare schemas ────────────────────────
                if not is_schema_changed(subject, new_schema_str):
                    logger.info(f"     ⏭️  Schema unchanged — Skipping")
                    registered[topic] = existing["schema_id"]
                    skipped += 1
                    continue

                logger.info(f"     🔄 Schema changed — Checking compatibility...")

                # ── Step 4: Compatibility check ────────────────────
                if not check_compatibility(subject, new_schema_str):
                    logger.error(f"     ❌ Incompatible schema — Skipping [{subject}]")
                    failed += 1
                    continue

                logger.info(f"     ✅ Compatible — Registering new version...")

            else:
                logger.info(f"     🆕 No existing schema — Registering fresh...")

            # ── Step 5: Register schema ────────────────────────────
            schema_id         = register_schema(subject, new_schema_str)
            registered[topic] = schema_id

        except FileNotFoundError:
            logger.error(f"     ❌ Schema file not found : {schema_path}")
            failed += 1
        except SchemaRegistryError as e:
            logger.error(f"     ❌ Schema Registry error  : {e}")
            failed += 1
        except Exception as e:
            logger.error(f"     ❌ Unexpected error        : {e}")
            failed += 1

    # ── Summary ────────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info(f"  📊 Total    : {len(TOPIC_SCHEMA_MAP)}")
    logger.info(f"  ✅ Registered : {len(registered) - skipped}")
    logger.info(f"  ⏭️  Skipped   : {skipped}")
    logger.info(f"  ❌ Failed    : {failed}")
    logger.info("=" * 60 + "\n")

    return registered
