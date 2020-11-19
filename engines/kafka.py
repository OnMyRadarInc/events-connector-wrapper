import json
import logging
import sys
from event_utils.abstract import AbstractConsumer, AbstractProducer
from confluent_kafka import Consumer, Producer

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class TopicEventMapper(object):
    mapper = {}

    def __init__(self, topic: str, events: list):
        self.__topic_name = topic
        self.mapper = {
            topic: {}
        }
        for event in events:
            self.mapper[topic][event] = event

    def get_event(self, event_name):
        """
        Returns an event given the event name
        :param event_name: String with the type of event
        :return: String with the mapped event
        """
        return self.mapper[self.__topic_name][event_name]

    def get_topic(self):
        """
        Returns topic name
        :return: String with the topic name
        """
        return self.__topic_name


class KafkaConfig:
    """
    Class with init function doing basic configuration for kafka service
    """
    def __init__(self, CLOUDKARAFKA_BROKERS, CLOUDKARAFKA_PASSWORD, CLOUDKARAFKA_USERNAME):
        self.username = CLOUDKARAFKA_USERNAME
        self.conf = {
            'bootstrap.servers': CLOUDKARAFKA_BROKERS,
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'latest'},
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-256',
            'sasl.username': CLOUDKARAFKA_USERNAME,
            'sasl.password': CLOUDKARAFKA_PASSWORD,
            'auto.offset.reset': 'earliest',
            'partition.assignment.strategy': 'roundrobin'
        }

    @staticmethod
    def create_mapper_class(topic: str, events: list) -> TopicEventMapper:
        """
        Creates a topic mapper class
        :param topic: String with the name of the topic
        :param events: List of strings with the events
        :return: TopicEventsMapper class
        """
        return TopicEventMapper(topic, events)

class KafkaConsumer(AbstractConsumer, KafkaConfig):
    """
    Class to create a kafka consumer
    """
    def __init__(self, CLOUDKARAFKA_BROKERS, CLOUDKARAFKA_PASSWORD, CLOUDKARAFKA_USERNAME, GROUP_ID):
        super().__init__(CLOUDKARAFKA_BROKERS, CLOUDKARAFKA_PASSWORD, CLOUDKARAFKA_USERNAME)
        self.conf['group.id'] = "%s-consumer" % (GROUP_ID)
        self.kafka_consumer = Consumer(**self.conf)

    def assign_topic(self, topics):
        """
        Assign a topic for the kafka_consumer
        :param topics: Name of the topic
        """
        final_topics = []
        for item in topics:
            final_topics.append(self.username + '-' + item)
        self.kafka_consumer.subscribe(final_topics)

    def get_message(self, timeout=None):
        return self.kafka_consumer.poll(timeout=timeout)

    @staticmethod
    def parameters_from_msg(msg):
        """
        Get the parameters on the header of a message
        :param msg:
        :return:
        """
        headers = msg.headers()
        value = str(msg.value(), "utf-8")

        json_message = json.loads(value)
        event = json_message['event']
        parameters = json_message['body']

        if not headers:
            return parameters, event

        for header in headers:
            parameters[header[0]] = str(header[1], "utf-8")

        return parameters, event


class KafkaProducer(AbstractProducer, KafkaConfig):
    """
    Class to create a kafka producer
    """
    def __init__(self, CLOUDKARAFKA_BROKERS, CLOUDKARAFKA_PASSWORD, CLOUDKARAFKA_USERNAME, GROUP_ID=None):
        super().__init__(CLOUDKARAFKA_BROKERS, CLOUDKARAFKA_PASSWORD, CLOUDKARAFKA_USERNAME)
        if GROUP_ID:
            self.conf['group.id'] = "%s-consumer" % (GROUP_ID)
        self.kafka_producer = Producer(**self.conf)

    @staticmethod
    def message_log(err, msg):
        """
        Gets an error and a message and handles the current message log
        :param err: Error, its None if the message was correctly sent
        :param msg: Message object from kafka
        """
        if err is not None:
            logger.error("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
        else:
            logger.info("Message produced: %s" % (str(msg.value())))


    def send_message(self, topic: str, event: str, body: dict = None, headers: dict = None, withLog: bool = False):
        """
        Sends a message to for given topic to the kafka service
        :param event: String with the event to be send to the kafka service. Ie. 'some message'
        :param topic: String with the topic of the message. Ie. 'introduction-topic'
        :param body: Dict with the body of the message
        :param headers: Dict with additional headers for the message. Ie. {'header1': 'value1', 'header2': 'value2'}
        """
        msg_headers = []
        message = {
            'event': event,
            'body': body
        }
        if headers:
            for header in headers.keys():
                msg_headers.append((
                    header,
                    bytes(headers[header], encoding='utf8')
                ))
        try:
            topic = self.username + '-' + topic
            json_message = json.dumps(message)
            args = {
                'headers': msg_headers,
            }
            if withLog:
                args['callback'] = self.message_log
            self.kafka_producer.produce(topic, json_message, **args)
            self.kafka_producer.flush()
        except BufferError as e:
            raise