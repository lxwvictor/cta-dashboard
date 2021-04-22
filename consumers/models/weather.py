"""Contains functionality related to Weather"""
import logging
import json


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #

        # The type of message.value() is dictionary, not a json string
        try:
            self.temperature = message.value()['temperature']
            self.status = message.value()['status']

            logger.info('processed weather, %s, %s' % (self.temperature, self.status))
        except:
            logger.warning("weather process_message is incomplete - skipping")