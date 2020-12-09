import json

from hege.bcscore.bgpatom_loader import BGPAtomLoader
from hege.bcscore.viewpoint import ViewPoint
import utils

with open("/app/config.json", "r") as f:
    config = json.load(f)
DUMP_INTERVAL = config["bcscore"]["dump_interval"]
BCSCORE_META_DATA_TOPIC = config["bcscore"]["meta_data_topic"]


class BCScoreBuilder:
    def __init__(self, collector: str, start_timestamp: int, end_timestamp: int):
        self.collector = collector
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp

        self.kafka_data_topic = f"ihr_bcscore_{collector}"
        self.kafka_meta_data_topic = BCSCORE_META_DATA_TOPIC

    def consume_and_calculate(self):
        for current_timestamp in range(self.start_timestamp, self.end_timestamp, DUMP_INTERVAL):
            bgpatom = self.load_bgpatom(current_timestamp)
            yield current_timestamp, self.get_viewpoint_bcscore_generator(bgpatom)

    def get_viewpoint_bcscore_generator(self, bgpatom: dict):
        for peer_address in bgpatom:
            peer_bgpatom = bgpatom[peer_address]
            peer_bcscore = self.calculate_viewpoint_bcscore(peer_bgpatom, peer_bgpatom)
            for origin_asn in peer_bcscore:
                yield peer_bcscore[origin_asn], peer_address

    def load_bgpatom(self, atom_timestamp):
        bgpatom = BGPAtomLoader(self.collector, atom_timestamp).load_bgpatom()
        return bgpatom

    @staticmethod
    def calculate_viewpoint_bcscore(bgpatom: dict, peer_address: str):
        viewpoint = ViewPoint(peer_address, bgpatom)
        return viewpoint.calculate_viewpoint_bcscore()


if __name__ == "__main__":
    start_at_time_string = "2020-08-01T00:00:00"
    start_at = utils.str_datetime_to_timestamp(start_at_time_string)

    end_at_time_string = "2020-08-01T00:16:00"
    end_at = utils.str_datetime_to_timestamp(end_at_time_string)

    test_collector = "rrc10"

    bcscore_builder = BCScoreBuilder(test_collector, start_at, end_at)
    for timestamp, viewpoint_bcscore_generator in bcscore_builder.consume_and_calculate():
        print(timestamp)
        for bcscore, peer in viewpoint_bcscore_generator:
            print(peer, bcscore)
            break
        break
