from collections import defaultdict
import logging
import json

from hege.utils import kafka_data, utils
from hege.utils.data_loader import DataLoader

try:
    with open("/app/config.json", "r") as f:
        config = json.load(f)
except FileNotFoundError:
    with open("./config.json", "r") as f:
        config = json.load(f)

config = config["bcscore"]

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
        ases_bcscore = message["bcscore"]
        scope = message["scope"]
        peer_asn = str(message["peer_asn"])

        for asn in ases_bcscore:
            value = (peer_asn, ases_bcscore[asn])
            bcscore[scope][asn].append(value)

        self.messages_per_peer[peer_asn] += 1


if __name__ == "__main__":
    bcscore_time_string = "2020-08-01T00:00:00"
    bcscore_timestamp = utils.str_datetime_to_timestamp(bcscore_time_string)

    test_collector = "rrc10"
    loaded_bcscore = BCSCORELoader(test_collector, bcscore_timestamp, True).load_data()

    print(f"completed: bcscore loaded at {bcscore_time_string} for {test_collector}")
    print(f"number of asn: {len(loaded_bcscore)}")
    for test_scope in loaded_bcscore:
        print(f"scope: {test_scope}")
        for depended_asn in loaded_bcscore[test_scope]:
            print(f"depended_asn: {depended_asn} \t peer_count: {len(loaded_bcscore[test_scope][depended_asn])}")
        break
