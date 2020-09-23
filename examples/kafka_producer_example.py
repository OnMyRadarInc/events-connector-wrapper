import time
from event_utils.event_utils import EventProducer

config= {
    'CLOUDKARAFKA_BROKERS': '{brokers list separated by commas}',
    'CLOUDKARAFKA_PASSWORD': '{password}',
    'CLOUDKARAFKA_USERNAME': '{username}'
}

kafka_producer = EventProducer('kafka', config).create_producer()

count = 0
while True:
    print("sending message")
    kafka_producer.send_message('introduction_api', 'TEST-' + str(count))
    count += 1
    time.sleep(5)