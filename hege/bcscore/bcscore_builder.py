import json
import math

from hege.bgpatom.bgpatom_loader import BGPAtomLoader
from hege.bcscore.viewpoint import ViewPoint
from hege.utils import utils

with open("/app/config.json", "r") as f:
    config = json.load(f)
DUMP_INTERVAL = config["bcscore"]["dump_interval"]
AS_BCSCORE_META_DATA_TOPIC = config["bcscore"]["meta_data_topic__as"]
AS_BCSCORE_DATA_TOPIC = config["bcscore"]["data_topic__as"]
PREFIX_BCSCORE_META_DATA_TOPIC = config["bcscore"]["meta_data_topic__prefix"]
PREFIX_BCSCORE_DATA_TOPIC = config["bcscore"]["data_topic__prefix"]


class BCScoreBuilder:
    def __init__(self, collector: str, start_timestamp: int, end_timestamp: int, prefix_mode=False):
        self.collector = collector
        self.start_timestamp = math.ceil(start_timestamp/DUMP_INTERVAL) * DUMP_INTERVAL
        self.end_timestamp = end_timestamp
        self.prefix_mode = prefix_mode

        if prefix_mode:
            self.kafka_data_topic = f"{PREFIX_BCSCORE_DATA_TOPIC}_{collector}"
            self.kafka_meta_data_topic = f"{PREFIX_BCSCORE_META_DATA_TOPIC}_{collector}"
        else:
            self.kafka_data_topic = f"{AS_BCSCORE_DATA_TOPIC}_{collector}"
            self.kafka_meta_data_topic = f"{AS_BCSCORE_META_DATA_TOPIC}_{collector}"

    def consume_and_calculate(self):
        for current_timestamp in range(self.start_timestamp, self.end_timestamp, DUMP_INTERVAL):
            bgpatom = self.load_bgpatom(current_timestamp)
            yield current_timestamp, self.get_viewpoint_bcscore_generator(bgpatom, current_timestamp)

    def get_viewpoint_bcscore_generator(self, bgpatom: dict, atom_timestamp: int):
        for peer_address in bgpatom:
            peer_bgpatom = bgpatom[peer_address]
            peer_bcscore_generator = self.calculate_viewpoint_bcscore(peer_bgpatom, peer_address, atom_timestamp)
            for bcscore_dump_data in peer_bcscore_generator:
                yield bcscore_dump_data, peer_address

    def load_bgpatom(self, atom_timestamp):
        bgpatom = BGPAtomLoader(self.collector, atom_timestamp).load_data()
        return bgpatom

    def calculate_viewpoint_bcscore(self, bgpatom: dict, peer_address: str, atom_timestamp: int):
        viewpoint = ViewPoint(peer_address, self.collector, bgpatom, atom_timestamp, self.prefix_mode)
        return viewpoint.calculate_viewpoint_bcscore()


if __name__ == "__main__":
    start_at_time_string = "2020-08-01T00:00:00"
    start_at = utils.str_datetime_to_timestamp(start_at_time_string)

    end_at_time_string = "2020-08-01T00:01:00"
    end_at = utils.str_datetime_to_timestamp(end_at_time_string)

    test_collector = "rrc10"

    bcscore_builder = BCScoreBuilder(test_collector, start_at, end_at)
    for timestamp, viewpoint_bcscore_generator in bcscore_builder.consume_and_calculate():
        print(timestamp)
        for bcscore, peer in viewpoint_bcscore_generator:
            print(peer, bcscore)
            break
        break
