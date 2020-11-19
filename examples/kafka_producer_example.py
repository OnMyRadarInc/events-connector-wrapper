import logging
import sys
import time
from event_utils.event_utils import EventProducer

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

CLOUDKARAFKA_BROKERS='{brokers list separated by commas}'
CLOUDKARAFKA_PASSWORD='{password}'
CLOUDKARAFKA_USERNAME='{username}'

CLOUDKARAFKA_BROKERS="omnibus-01.srvs.cloudkafka.com:9094,omnibus-02.srvs.cloudkafka.com:9094,omnibus-03.srvs.cloudkafka.com:9094"
CLOUDKARAFKA_PASSWORD="CzWYKc_plGbiQhF5inyh_q9T0nQXWAfA"
CLOUDKARAFKA_USERNAME="uk811k5s"
GROUP_ID='GROUPA'

config= {
    'CLOUDKARAFKA_BROKERS': CLOUDKARAFKA_BROKERS,
    'CLOUDKARAFKA_PASSWORD': CLOUDKARAFKA_PASSWORD,
    'CLOUDKARAFKA_USERNAME': CLOUDKARAFKA_USERNAME,
}

kafka_producer = EventProducer('kafka', config).create_producer()
TEST = kafka_producer.create_mapper_class('TEST', ['TEST-A', 'TEST-B'])

count = 0
while True:
    logger.info("sending message")
    body = {"something": 'a'}
    kafka_producer.send_message(TEST.get_topic(), TEST.get_event('TEST-A'), body, withLog=True)
    count += 1
    time.sleep(2)