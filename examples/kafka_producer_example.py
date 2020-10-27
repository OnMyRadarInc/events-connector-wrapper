import logging
import sys
import time
from event_utils.event_utils import EventProducer

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

CLOUDKARAFKA_BROKERS='{brokers list separated by commas}'
CLOUDKARAFKA_PASSWORD='{password}'
CLOUDKARAFKA_USERNAME='{username}'

config= {
    'CLOUDKARAFKA_BROKERS': CLOUDKARAFKA_BROKERS,
    'CLOUDKARAFKA_PASSWORD': CLOUDKARAFKA_PASSWORD,
    'CLOUDKARAFKA_USERNAME': CLOUDKARAFKA_USERNAME,
}

kafka_producer = EventProducer('kafka', config).create_producer()

count = 0
while True:
    logger.info("sending message")
    kafka_producer.send_message('TEST', 'TEST-' + str(count))
    count += 1
    time.sleep(2)