from collections import defaultdict
import logging
import json

from hege.utils import kafka_data, utils
from hege.utils.data_loader import DataLoader

with open("/app/config.json", "r") as f:
    config = json.load(f)["bcscore"]
BCSCORE_DATA_TOPIC = config["data_topic"]
BCSCORE_META_DATA_TOPIC = config["meta_data_topic"]


class BCSCORELoader(DataLoader):
    def __init__(self, timestamp: int):
        super().__init__(timestamp)
        self.topic = BCSCORE_DATA_TOPIC
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

    def cross_check_with_meta_data(self):
        consumer = kafka_data.create_consumer_and_set_offset(BCSCORE_META_DATA_TOPIC, self.timestamp)
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
    bcscore_time_string = "2020-08-01T00:00:00"
    bcscore_timestamp = utils.str_datetime_to_timestamp(bcscore_time_string)

    loaded_bcscore = BCSCORELoader(bcscore_timestamp).load_data()

    print(f"completed: bcscore loaded at {bcscore_time_string}")
    print(f"number of asn: {len(loaded_bcscore)}")
    for scope in loaded_bcscore:
        print(f"scope asn: {scope}")
        for depended_asn in loaded_bcscore[scope]:
            print(f"depended_asn: {depended_asn} \t peer_count: {len(loaded_bcscore[scope][depended_asn])}")
        break
