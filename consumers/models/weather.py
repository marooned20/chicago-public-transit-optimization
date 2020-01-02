"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """
        Handles incoming weather data
        """
        # Process incoming weather messages. Set the temperature and status.
        weather_message = message.value()
        self.temperature = weather_message["temperature"]
        self.status = weather_message["status"]
        logger.info(f"Temperature: {self.temperature}, Status: {self.status}")
