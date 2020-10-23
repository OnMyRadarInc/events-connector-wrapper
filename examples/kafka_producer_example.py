import time
from event_utils.event_utils import EventProducer

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
    print("sending message")
    kafka_producer.send_message('TEST', 'TEST-' + str(count))
    count += 1
    time.sleep(2)