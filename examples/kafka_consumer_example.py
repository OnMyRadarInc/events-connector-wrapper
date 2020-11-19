import logging
import sys
import time
from event_utils.event_utils import EventConsumer

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

CLOUDKARAFKA_BROKERS='{brokers list separated by commas}'
CLOUDKARAFKA_PASSWORD='{password}'
CLOUDKARAFKA_USERNAME='{username}'
GROUP_ID='{id of the group}'

CLOUDKARAFKA_BROKERS="omnibus-01.srvs.cloudkafka.com:9094,omnibus-02.srvs.cloudkafka.com:9094,omnibus-03.srvs.cloudkafka.com:9094"
CLOUDKARAFKA_PASSWORD="CzWYKc_plGbiQhF5inyh_q9T0nQXWAfA"
CLOUDKARAFKA_USERNAME="uk811k5s"
GROUP_ID='GROUPA'

config= {
    'CLOUDKARAFKA_BROKERS': CLOUDKARAFKA_BROKERS,
    'CLOUDKARAFKA_PASSWORD': CLOUDKARAFKA_PASSWORD,
    'CLOUDKARAFKA_USERNAME': CLOUDKARAFKA_USERNAME,
    'GROUP_ID': GROUP_ID
}

kafka_consumer = EventConsumer('kafka', config).create_consumer()
TEST = kafka_consumer.create_mapper_class('TEST', ['TEST-A', 'TEST-B'])

topics = [TEST.get_topic()]
kafka_consumer.assign_topic(topics)

while True:
    msg = kafka_consumer.get_message(timeout=5.0)
    if msg:
        logger.info("=====New Message")
        parameters, value = kafka_consumer.parameters_from_msg(msg)
        logger.info("Params: " + str(parameters))
        logger.info("Value: " + value)
    else:
        print("No message")
    time.sleep(3)