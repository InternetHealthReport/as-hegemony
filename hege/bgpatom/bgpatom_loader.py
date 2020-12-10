from collections import defaultdict
import logging
import json

from hege.utils import kafka_data, utils
from hege.utils.data_loader import DataLoader

with open("/app/config.json", "r") as f:
    config = json.load(f)["bgpatom"]
BGPATOM_META_DATA_TOPIC = config["meta_data_topic"]


class BGPAtomLoader(DataLoader):
    def __init__(self, collector: str, timestamp: int):
        super().__init__(timestamp)
        self.collector = collector
        self.topic = f"ihr_bgp_atom_{collector}"
        logging.debug(f"start consuming from {self.topic} at {timestamp}")

    @staticmethod
    def prepare_load_data():
        return defaultdict(lambda: defaultdict(list))

    def prepare_consumer(self):
        return kafka_data.create_consumer_and_set_offset(self.topic, self.timestamp)

    def read_message(self, message: dict, collector_bgpatom: dict):
        peer_address = message["peer_address"]
        as_path = tuple(message["aspath"])
        prefixes = message["prefixes"]

        peer_bgpatom = collector_bgpatom[peer_address]
        peer_bgpatom[as_path] += prefixes

        self.messages_per_peer[peer_address] += 1

    def cross_check_with_meta_data(self):
        consumer = kafka_data.create_consumer_and_set_offset(BGPATOM_META_DATA_TOPIC, self.timestamp)
        messages_per_peer = defaultdict(int)

        for message, _ in kafka_data.consume_stream(consumer):
            message_timestamp = message["timestamp"]
            if message_timestamp != self.timestamp:
                break
            meta = message["messages_per_peer"]
            for peer_address in meta:
                messages_per_peer[peer_address] += meta[peer_address]

        for peer_address in self.messages_per_peer:
            if messages_per_peer[peer_address] != self.messages_per_peer[peer_address]:
                logging.error(f"number of messages received is different from messages in metadata"
                              f"(meta: {messages_per_peer[peer_address]}, "
                              f"received: {self.messages_per_peer[peer_address]}, "
                              f"peer_address: {peer_address})")
                return False

        return True


if __name__ == "__main__":
    bgpatom_time_string = "2020-08-01T00:00:00"
    bgpatom_timestamp = utils.str_datetime_to_timestamp(bgpatom_time_string)

    test_collector = "rrc10"
    bgpatom_loader = BGPAtomLoader(test_collector, bgpatom_timestamp)
    bgpatom = bgpatom_loader.load_data()

    print(f"completed: {test_collector} loaded at {bgpatom_time_string}")
    print(f"number of peers: {len(bgpatom)}")
    for peer in bgpatom:
        print(f"number of atoms in {peer}: {len(bgpatom[peer])}")
