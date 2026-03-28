import logging
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.error import SchemaRegistryError

from config.settings import KAFKA_BROKER, SCHEMA_REGISTRY_URL, TOPIC_SCHEMA_MAP
from producer.topic_manager import ensure_topic_exists
from producer.schema_validator import validate_records
from schema.register_client.registry_client import load_schema

logger = logging.getLogger(__name__)


# ── Delivery Report ───────────────────────────────────────────
def delivery_report(err, msg):
    if err:
        logger.error(f"  ❌ Delivery failed  : {err}")
    else:
        logger.info(
            f"  ✅ Delivered → Topic: [{msg.topic()}] | "
            f"Partition: {msg.partition()} | Offset: {msg.offset()}"
        )


# ── Build Avro Serializer ─────────────────────────────────────
def get_avro_serializer(topic: str) -> AvroSerializer:
    """Build an AvroSerializer for the given topic's schema."""
    schema_path = TOPIC_SCHEMA_MAP[topic]
    schema_str  = load_schema(schema_path)

    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    return AvroSerializer(
        schema_registry_client = sr_client,
        schema_str             = schema_str,
        to_dict                = lambda obj, ctx: obj,
    )


# ── Build Kafka Producer ──────────────────────────────────────
def get_kafka_producer() -> Producer:
    """Create and return a Kafka Producer."""
    return Producer({
        "bootstrap.servers"  : KAFKA_BROKER,
        "acks"               : "all",           # Wait for all replicas
        "retries"            : 3,               # Retry on failure
        "retry.backoff.ms"   : 500,
        "linger.ms"          : 5,               # Batch messages slightly
        "compression.type"   : "snappy",        # Compress messages
    })


# ── Main Produce Function ─────────────────────────────────────
def produce_records(
    topic      : str,
    records    : list,
    key_field  : str  = "id",
    skip_invalid: bool = True,
) -> dict:
    """
    Full pipeline to produce records to a Kafka Avro topic:
      1. Ensure topic exists (create if not)
      2. Validate records against Avro schema
      3. Produce valid records to Kafka topic
      4. Return summary report

    Args:
        topic        : Kafka topic name
        records      : List of dict records to produce
        key_field    : Field to use as Kafka message key
        skip_invalid : If True, skip invalid records; else raise error

    Returns:
        {
          "topic"           : str,
          "total"           : int,
          "valid"           : int,
          "invalid"         : int,
          "produced"        : int,
          "failed"          : int,
          "invalid_records" : list,
        }
    """
    logger.info("=" * 60)
    logger.info(f"  🚀 Produce Pipeline Started : [{topic}]")
    logger.info("=" * 60)

    report = {
        "topic"           : topic,
        "total"           : len(records),
        "valid"           : 0,
        "invalid"         : 0,
        "produced"        : 0,
        "failed"          : 0,
        "invalid_records" : [],
    }

    try:
        # ── Step 1: Ensure topic exists ────────────────────────
        ensure_topic_exists(topic)

        # ── Step 2: Validate records against schema ────────────
        valid_records, invalid_records = validate_records(topic, records)

        report["valid"]           = len(valid_records)
        report["invalid"]         = len(invalid_records)
        report["invalid_records"] = invalid_records

        if invalid_records and not skip_invalid:
            raise ValueError(
                f"{len(invalid_records)} invalid record(s) found. "
                f"Set skip_invalid=True to skip them."
            )

        if not valid_records:
            logger.warning("  ⚠️  No valid records to produce. Exiting.")
            return report

        # ── Step 3: Build producer & serializer ────────────────
        producer        = get_kafka_producer()
        avro_serializer = get_avro_serializer(topic)

        logger.info(f"\n  📤 Producing {len(valid_records)} valid record(s)...")

        # ── Step 4: Produce messages ───────────────────────────
        produced = 0
        failed   = 0

        for record in valid_records:
            try:
                serialized_value = avro_serializer(
                    record,
                    SerializationContext(topic, MessageField.VALUE)
                )
                producer.produce(
                    topic       = topic,
                    key         = str(record.get(key_field, "")),
                    value       = serialized_value,
                    on_delivery = delivery_report,
                )
                producer.poll(0)    # Trigger delivery callbacks
                produced += 1

            except SchemaRegistryError as e:
                logger.error(f"  ❌ Serialization error: {e}")
                failed += 1
            except Exception as e:
                logger.error(f"  ❌ Produce error: {e}")
                failed += 1

        # ── Step 5: Flush all messages ─────────────────────────
        logger.info(f"\n  ⏳ Flushing messages to Kafka...")
        producer.flush()

        report["produced"] = produced
        report["failed"]   = failed

    except Exception as e:
        logger.error(f"  ❌ Pipeline error: {e}")
        raise

    finally:
        # ── Step 6: Print Summary ──────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info(f"  📊 Produce Summary for [{topic}]")
        logger.info(f"  ─────────────────────────────────────────────")
        logger.info(f"  📥 Total Records    : {report['total']}")
        logger.info(f"  ✅ Valid Records    : {report['valid']}")
        logger.info(f"  ❌ Invalid Records  : {report['invalid']}")
        logger.info(f"  📤 Produced         : {report['produced']}")
        logger.info(f"  💥 Failed           : {report['failed']}")
        logger.info("=" * 60 + "\n")

    return report
