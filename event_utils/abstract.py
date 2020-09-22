from abc import abstractmethod


class AbstractConsumer:
    @abstractmethod
    def assign_topic(self, topics):
        pass

    @abstractmethod
    def get_message(self, timeout):
        pass


class AbstractProducer:
    @abstractmethod
    def send_message(self, topic, message, headers):
        pass