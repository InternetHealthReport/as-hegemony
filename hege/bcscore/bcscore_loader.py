from collections import defaultdict
import logging
import json

from hege.utils import kafka_data, utils
from hege.utils.data_loader import DataLoader

with open("/app/config.json", "r") as f:
    config = json.load(f)["bcscore"]
AS_BCSCORE_DATA_TOPIC = config["data_topic__as"]
AS_BCSCORE_META_DATA_TOPIC = config["meta_data_topic__as"]
PREFIX_BCSCORE_DATA_TOPIC = config["data_topic__prefix"]
PREFIX_BCSCORE_META_DATA_TOPIC = config["meta_data_topic__prefix"]


class BCSCORELoader(DataLoader):
    def __init__(self, collector: str, timestamp: int, prefix_mode=False):
        super().__init__(timestamp)
        self.collector = collector
        self.prefix_mode = prefix_mode
        if prefix_mode:
            self.topic = f"{PREFIX_BCSCORE_DATA_TOPIC}_{collector}"
            self.metadata_topic = f"{PREFIX_BCSCORE_META_DATA_TOPIC}_{collector}"
        else:
            self.topic = f"{AS_BCSCORE_DATA_TOPIC}_{collector}"
            self.metadata_topic = f"{AS_BCSCORE_META_DATA_TOPIC}_{collector}"

        logging.debug(f"start consuming from {self.topic} at {self.timestamp}")

    @staticmethod
    def prepare_load_data():
        return defaultdict(lambda: defaultdict(list))

    def prepare_consumer(self):
        return kafka_data.create_consumer_and_set_offset(self.topic, self.timestamp)

    def read_message(self, message: dict, bcscore: dict):
        peer_asn = message["peer_asn"]
        peer_bcscore = message["bcscore"]
        peer_address = message["peer_address"]
        scope = message["scope"]

        for asn in peer_bcscore:
            value = (peer_asn, peer_bcscore[asn])
            bcscore[scope][asn].append(value)

        self.messages_per_peer[peer_address] += 1


if __name__ == "__main__":
    bcscore_time_string = "2020-08-01T00:00:00"
    bcscore_timestamp = utils.str_datetime_to_timestamp(bcscore_time_string)

    test_collector = "rrc10"
    loaded_bcscore = BCSCORELoader(test_collector, bcscore_timestamp, True).load_data()

    print(f"completed: bcscore loaded at {bcscore_time_string} for {test_collector}")
    print(f"number of asn: {len(loaded_bcscore)}")
    for scope in loaded_bcscore:
        print(f"scope: {scope}")
        for depended_asn in loaded_bcscore[scope]:
            print(f"depended_asn: {depended_asn} \t peer_count: {len(loaded_bcscore[scope][depended_asn])}")
        break
