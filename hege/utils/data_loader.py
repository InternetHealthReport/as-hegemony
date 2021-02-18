from collections import defaultdict
import logging

from hege.utils import kafka_data


class DataLoader:
    def __init__(self, timestamp: int):
        self.timestamp = timestamp
        self.messages_per_peer = defaultdict(int)
        self.metadata_topic = None

    def load_data(self):
        loading_data = self.prepare_load_data()
        consumer = self.prepare_consumer()

        for message, _ in kafka_data.consume_stream(consumer, self.timestamp):
            # message_timestamp = message["timestamp"]
            # if message_timestamp != self.timestamp:
                # break
            self.read_message(message, loading_data)

        # self.cross_check_with_meta_data()
        return loading_data

    def prepare_load_data(self):
        raise NotImplementedError

    def prepare_consumer(self):
        raise NotImplementedError

    def read_message(self, message: dict, collector_bgpatom: dict):
        raise NotImplementedError

    def cross_check_with_meta_data(self):
        consumer = kafka_data.create_consumer_and_set_offset(self.metadata_topic, self.timestamp)
        messages_per_peer = defaultdict(int)

        for message, _ in kafka_data.consume_stream(consumer, self.timestamp):
            # message_timestamp = message["timestamp"]
            # if message_timestamp != self.timestamp:
                # break
            meta = message["messages_per_peer"]
            for key in meta:
                messages_per_peer[key] += meta[key]

        for key in self.messages_per_peer:
            if messages_per_peer[key] != self.messages_per_peer[key]:
                logging.error(f"number of messages received is different from messages in metadata ( "
                              f"meta: {messages_per_peer[key]}, "
                              f"received: {self.messages_per_peer[key]}, "
                              f"key: {key} )")
                continue
