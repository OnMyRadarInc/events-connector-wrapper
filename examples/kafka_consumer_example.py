import time
from event_utils.event_utils import EventConsumer

config= {
    'CLOUDKARAFKA_BROKERS': '{brokers list separated by commas}',
    'CLOUDKARAFKA_PASSWORD': '{password}',
    'CLOUDKARAFKA_USERNAME': '{username}'
}

kafka_consumer = EventConsumer('kafka', config).create_consumer()

topics = ['introduction_api']
kafka_consumer.assign_topic(topics)

while True:
    msg = kafka_consumer.get_message(timeout=5.0)
    if msg:
        print(msg.value())
    else:
        print("No message")
    time.sleep(5)