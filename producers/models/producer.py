"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

# define constants
BOOTSTRAP_SERVERS = "PLAINTEXT://kafka0:19092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081/"


class Producer:
    """
    Defines and provides common functionality amongst Producers
    """

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(self, topic_name, key_schema, value_schema=None, num_partitions=1, num_replicas=1):
        """
        Initializes a Producer object with basic settings
        """
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # Configure the broker properties Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        self.broker_properties = {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            logger.info(f"Topic not in existing topics. Creating {topic_name}")
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=key_schema,
            default_value_schema=value_schema,
        )

    def create_topic(self):
        """
        Creates the producer topic if it does not already exist
        """
        # define client
        client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

        is_topic = self.topic_exists(client, self.topic_name)

        if is_topic:
            logger.info(
                f"topic {self.topic_name} exists. Skipping creation...")
            return
        else:
            logger.info(f"creating topic: {self.topic_name}")

            futures = client.create_topics(
                [
                    NewTopic(
                        topic=self.topic_name,
                        num_partitions=self.num_partitions,
                        replication_factor=self.num_replicas,
                        # NOTE: config parameters can be added here. e.g. compression type etc
                    )
                ]
            )

            for topic, future in futures.items():
                try:
                    future.result()
                    print("topic created")
                except Exception as error:
                    print(f"failed to create topic {self.topic_name}: {error}")
                    raise

    def topic_exists(self, client, topic_name):
        """
        Checks if the given topic exists
        """

        topic_metadata = client.list_topics(timeout=30)

        return topic_metadata.topics.get(topic_name) is not None

    def close(self):
        """
        Prepares the producer for exit by cleaning up the producer
        """

        if self.producer is None:
            logger.debug(f"producer is None, no need to flush")
            return

        logger.debug("Flush the producer")
        self.producer.flush()

    def time_millis(self):
        """
        Use this function to get the key for Kafka Events
        """
        return int(round(time.time() * 1000))
