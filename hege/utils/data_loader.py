from collections import defaultdict

from hege.utils import kafka_data


class DataLoader:
    def __init__(self, timestamp: int):
        self.timestamp = timestamp
        self.messages_per_peer = defaultdict(int)

    def load_data(self):
        loading_data = self.prepare_load_data()
        consumer = self.prepare_consumer()

        for message, _ in kafka_data.consume_stream(consumer):
            message_timestamp = message["timestamp"]
            if message_timestamp != self.timestamp:
                break
            self.read_message(message, loading_data)

        if self.cross_check_with_meta_data():
            return loading_data
        else:
            return dict()

    def prepare_load_data(self):
        raise NotImplementedError

    def prepare_consumer(self):
        raise NotImplementedError

    def read_message(self, message: dict, collector_bgpatom: dict):
        raise NotImplementedError

    def cross_check_with_meta_data(self):
        raise NotImplementedError
