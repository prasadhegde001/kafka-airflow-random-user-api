import json
import logging
import fastavro
import io
from fastavro.schema import parse_schema
from config.settings import TOPIC_SCHEMA_MAP

logger = logging.getLogger(__name__)


def load_parsed_schema(topic):
    """Load and parse the Avro schema for a given topic."""
    schema_path = TOPIC_SCHEMA_MAP.get(topic)
    if not schema_path:
        raise ValueError(f"No schema mapped for topic: [{topic}]")

    with open(schema_path, "r") as f:
        raw_schema = json.load(f)

    return parse_schema(raw_schema)


def validate_record(record, parsed_schema):
    """
    Validate a single record against the parsed Avro schema.
    Returns: (is_valid: bool, error_message: str)
    """
    try:
        # Try writing the record using fastavro
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, parsed_schema, record)
        return True, ""
    except Exception as e:
        return False, str(e)


def validate_records(topic, records):
    """
    Validate all records against the topic's Avro schema.

    Returns:
        valid_records   : list of records that match schema
        invalid_records : list of dicts with record + error info
    """
    logger.info(f"\n  Validating {len(records)} record(s) for topic [{topic}]...")

    parsed_schema   = load_parsed_schema(topic)

    print(f"Parsed Schema is {parsed_schema}")

    valid_records   = []
    invalid_records = []

    for idx, record in enumerate(records):
        print(f"record is {record}")
        is_valid, error = validate_record(record, parsed_schema)

        if is_valid:
            valid_records.append(record)
            logger.debug(f"  Record [{idx}] : Valid")
        else:
            invalid_records.append({
                "index"  : idx,
                "record" : record,
                "error"  : error,
            })
            logger.warning(f"  Record [{idx}] : Invalid → {error}")

    # ── Validation Summary ─────────────────────────────────
    logger.info(f"  ─────────────────────────────────────────────")
    logger.info(f"  📊 Total    : {len(records)}")
    logger.info(f"  ✅ Valid    : {len(valid_records)}")
    logger.info(f"  ❌ Invalid  : {len(invalid_records)}")
    logger.info(f"  ─────────────────────────────────────────────")

    return valid_records, invalid_records