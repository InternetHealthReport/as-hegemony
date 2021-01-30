from collections import defaultdict
import logging
import json

from hege.utils import kafka_data, utils
from hege.utils.data_loader import DataLoader

with open("/app/config.json", "r") as f:
    config = json.load(f)["bgpatom"]
BGPATOM_DATA_TOPIC = config["data_topic"]
BGPATOM_META_DATA_TOPIC = config["meta_data_topic"]


class BGPAtomLoader(DataLoader):
    def __init__(self, collector: str, timestamp: int):
        super().__init__(timestamp)
        self.collector = collector
        self.topic = f"{BGPATOM_DATA_TOPIC}_{collector}"
        self.metadata_topic = f"{BGPATOM_META_DATA_TOPIC}_{collector}"
        logging.debug(f"start consuming from {self.topic} at {timestamp}")

    @staticmethod
    def prepare_load_data():
        return defaultdict(lambda: defaultdict(list))

    def prepare_consumer(self):
        return kafka_data.create_consumer_and_set_offset(self.topic, self.timestamp)

    def read_message(self, message: dict, collector_bgpatom: dict):
        peer_address = message["peer_address"]
        peer_asn = message["peer_asn"]
        as_path = tuple(message["aspath"])
        prefixes = message["prefixes"]

        peer_bgpatom = collector_bgpatom[(peer_address, peer_asn)]
        peer_bgpatom[as_path] += prefixes

        self.messages_per_peer[peer_address] += 1


if __name__ == "__main__":
    bgpatom_time_string = "2020-08-01T00:00:00"
    bgpatom_timestamp = utils.str_datetime_to_timestamp(bgpatom_time_string)

    test_collector = "rrc00"
    bgpatom_loader = BGPAtomLoader(test_collector, bgpatom_timestamp)
    bgpatom = bgpatom_loader.load_data()

    print(f"completed: {test_collector} loaded at {bgpatom_time_string}")
    print(f"number of peers: {len(bgpatom)}")
    for peer in bgpatom:
        print(f"number of atoms in {peer}: {len(bgpatom[peer])}")
