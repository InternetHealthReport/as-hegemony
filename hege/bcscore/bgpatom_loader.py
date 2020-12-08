from collections import defaultdict
import logging
import json

import kafkadata
import utils


with open("config.json", "r") as f:
    config = json.load(f)["bgpatom"]
BGPATOM_META_DATA_TOPIC = config["meta_data_topic"]


class BGPAtomLoader:
    def __init__(self, collector: str, timestamp: int):
        self.collector = collector
        self.timestamp = timestamp
        self.messages_per_peer = defaultdict(int)

    def load_bgpatom(self):
        collector_bgpatom = defaultdict(lambda: defaultdict(list))
        bgpatom_topic = f"ihr_bgp_atom_{self.collector}"
        consumer = kafkadata.create_consumer_and_set_offset(bgpatom_topic, self.timestamp)

        logging.debug(f"start consuming bgpatom from {bgpatom_topic} at {self.timestamp}")
        for message, _ in kafkadata.consume_stream(consumer):
            message_timestamp = message["timestamp"]
            if message_timestamp != self.timestamp:
                break
            self.read_bgpatom_message(message, collector_bgpatom)

        if self.cross_check_with_meta_data():
            return collector_bgpatom
        else:
            return dict()

    def read_bgpatom_message(self, message: dict, collector_bgpatom: dict):
        peer_address = message["peer_address"]
        as_path = tuple(message["aspath"])
        prefixes = message["prefixes"]

        peer_bgpatom = collector_bgpatom[peer_address]
        peer_bgpatom[as_path] += prefixes

        self.messages_per_peer[peer_address] += 1

    def cross_check_with_meta_data(self):
        consumer = kafkadata.create_consumer_and_set_offset(BGPATOM_META_DATA_TOPIC, self.timestamp)
        messages_per_peer = None
        for message, _ in kafkadata.consume_stream(consumer):
            message_timestamp = message["timestamp"]
            if message_timestamp != self.timestamp:
                break
            messages_per_peer = message["messages_per_peer"]

        if messages_per_peer is None:
            logging.error("did not find bgpatom metadata")
            return False

        for peer_address in self.messages_per_peer:
            if messages_per_peer[peer_address] != self.messages_per_peer[peer_address]:
                logging.error("number of messages received is different from messages in metadata")
                return False

        return True


if __name__ == "__main__":
    bgpatom_time_string = "2020-08-01T00:00:00"
    bgpatom_timestamp = utils.str_datetime_to_timestamp(bgpatom_time_string)

    test_collector = "rrc10"
    bgpatom = BGPAtomLoader(test_collector, bgpatom_timestamp).load_bgpatom()

    print(f"completed: {test_collector} loaded at {bgpatom_time_string}")
    print(f"number of peers: {len(bgpatom)}")
    for peer in bgpatom:
        print(f"number of atoms in {peer}: {len(bgpatom[peer])}")
