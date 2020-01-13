"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen

logger = logging.getLogger(__name__)

KAFKA_BROKER_URL = "PLAINTEXT://localhost:9092"


class KafkaConsumer:
    """
    Defines the base kafka consumer class
    """

    def __init__(self, topic_name_pattern, message_handler, is_avro=True, offset_earliest=False, sleep_secs=1.0, consume_timeout=0.1):
        """
        Creates a consumer object for asynchronous use
        """
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        # configure broker properties for consumer
        self.broker_properties = {
            "bootstrap.servers": KAFKA_BROKER_URL,
            "group.id": topic_name_pattern,
            "auto.offset.reset": "earliest"
        }

        # Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        # Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        self.consumer.subscribe(
            [self.topic_name_pattern],  # always a list
            on_assign=self.on_assign
        )

    def on_assign(self, consumer, partitions):
        """
        Callback for when topic assignment takes place
        """
        # If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest

        for partition in partitions:
            if self.offset_earliest == True:
                partition.offset = OFFSET_BEGINNING

        logger.info(f"partitions assigned for {self.topic_name_pattern}")
        consumer.assign(partitions)

    async def consume(self):
        """
        Asynchronously consumes data from kafka topic
        """
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""

        try:
            message = self.consumer.poll(timeout=self.consume_timeout)
        except:
            logger.error(f"Message poll failed for: {self.topic_name_pattern}")
            return 0

        if message is None:
            logger.info("No message received by consumer")
            return 0
        elif message.error() is not None:
            logger.error(f"Error from consumer: {message.error()}")
            return 0
        
        self.message_handler(message)
        logger.info(
            f"Consumed message - {message.key()}: {message.value()}")
        return 1

    def close(self):
        """
        Cleans up any open kafka consumers
        """
        logger.info(f"closing consumer for {self.topic_name_pattern}")
        self.consumer.close()
