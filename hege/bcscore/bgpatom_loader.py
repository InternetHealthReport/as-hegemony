import logging
from collections import defaultdict

import kafkadata
import utils


class BGPAtomLoader:
    def __init__(self, collector: str, timestamp: int):
        self.collector = collector
        self.timestamp = timestamp

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

        return collector_bgpatom

    @staticmethod
    def read_bgpatom_message(message: dict, collector_bgpatom: dict):
        peer_address = message["peer_address"]
        as_path = tuple(message["aspath"])
        prefixes = message["prefixes"]

        peer_bgpatom = collector_bgpatom[peer_address]
        peer_bgpatom[as_path] += prefixes


if __name__ == "__main__":
    bgpatom_time_string = "2020-08-01T00:00:00"
    bgpatom_datetime = utils.str_datetime_to_datetime(bgpatom_time_string, utils.DATETIME_STRING_FORMAT)
    bgpatom_timestamp = utils.datetime_to_timestamp(bgpatom_datetime)

    test_collector = "rrc10"
    bgpatom = BGPAtomLoader(test_collector, bgpatom_timestamp).load_bgpatom()

    print(f"completed: {test_collector} loaded at {bgpatom_time_string}")
    print(f"number of peers: {len(bgpatom)}")
    for peer in bgpatom:
        print(f"number of atoms in {peer}: {len(bgpatom[peer])}")
