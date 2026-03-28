import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException
from config.settings import KAFKA_BROKER, TOPIC_CONFIG

logger = logging.getLogger(__name__)


def get_admin_client():
    """Create and return a Kafka AdminClient."""
    return AdminClient({"bootstrap.servers": KAFKA_BROKER})

def get_existing_topics():
    """Fetch all existing topic names from Kafka."""
    client          = get_admin_client()
    metadata        = client.list_topics(timeout=10)
    existing_topics = set(metadata.topics.keys())
    return existing_topics

def topic_exists(topic):
    """Check if a topic already exists in Kafka."""
    return topic in get_existing_topics()

def create_topic(topic):
    """
    Create a Kafka topic if it doesn't already exist.
    Returns True if created, False if already exists.
    """
    client = get_admin_client()

    if topic_exists(topic):
        logger.info(f" Topic already exists : [{topic}]")
        return False
    new_topic = NewTopic(
        topic = topic,
        num_partitions = TOPIC_CONFIG['num_partitions'],
        replication_factor = TOPIC_CONFIG['replication_factor'],
        config = {
            "retention.ms"    : TOPIC_CONFIG["retention_ms"],
            "cleanup.policy"  : TOPIC_CONFIG["cleanup_policy"]
        }
    )

    futures = client.create_topics([new_topic])

    for topic_name, future in futures.items():
        try:
            future.result()  # Block until topic is created
            logger.info(f"  Topic created : [{topic_name}]")
            logger.info(f"     Partitions    : {TOPIC_CONFIG['num_partitions']}")
            logger.info(f"     Replication   : {TOPIC_CONFIG['replication_factor']}")
            return True
        except KafkaException as e:
            logger.error(f" Failed to create topic [{topic_name}]: {e}")
            raise

def ensure_topic_exists(topic):
    """
    Check if topic exists — create it if not.
    Called before producing messages.
    """
    logger.info(f"\n  Checking topic : [{topic}]")
    if not topic_exists(topic):
        logger.info(f"  Topic not found — Creating...")
        create_topic(topic)
    else:
        logger.info(f"  Topic exists   : [{topic}]")