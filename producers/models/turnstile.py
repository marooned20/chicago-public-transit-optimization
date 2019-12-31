"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware

logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        # with each unique station_name, a new topic will be created/assigned
        topic_name = f"com.udacity.{station_name}.turnstile"
        super().__init__(
            topic_name,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=5,  # can go into a separate config file instead of hard-coded
            num_replicas=1  # can go into a separate config file instead of hard-coded
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """
        Simulates riders entering through the turnstile.
        """
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.info(f"total entries in {self.station.name}: {num_entries}")

        for _ in range(num_entries):
            
            # TODO: change logging level later
            logger.info(f"producing to {self.topic_name}")

            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value={
                    "station_id": self.station.station_id,
                    "station_name": self.station.station_name,
                    "line": self.color.name,
                },
            )
