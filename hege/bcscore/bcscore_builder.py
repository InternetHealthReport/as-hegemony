import json

from hege.bcscore.bgpatom_loader import BGPAtomLoader
from hege.bcscore.viewpoint import ViewPoint
import utils

with open("../../config.json", "r") as f:
    config = json.load(f)
DUMP_INTERVAL = config["bcscore"]["dump_interval"]


class BCScoreBuilder:
    def __init__(self, collector: str, start_timestamp: int, end_timestamp: int):
        self.collector = collector
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.peer_address_to_viewpoints = dict()

    def load_bgpatom(self, atom_timestamp):
        bgpatom = BGPAtomLoader(self.collector, atom_timestamp).load_bgpatom()
        for peer_address in bgpatom:
            self.peer_address_to_viewpoints[peer_address] = ViewPoint(peer_address, bgpatom[peer_address])

    def get_viewpoint(self, peer_address):
        return self.peer_address_to_viewpoints[peer_address]

    def consume_bgpatom_and_calculate_bcscore(self):
        for current_timestamp in range(self.start_timestamp, self.end_timestamp, DUMP_INTERVAL):
            self.load_bgpatom(current_timestamp)


    def calculate_bcscore(self):
        for dump_timestamp in self.update_viewpoints_status_and_yield():
            print(self.calculate_bcscore_helper())
            return


if __name__ == "__main__":
    start_at_time_string = "2020-08-01T00:00:00"
    start_at_datetime = utils.str_datetime_to_datetime(start_at_time_string, utils.DATETIME_STRING_FORMAT)
    start_at = utils.datetime_to_timestamp(start_at_datetime)

    end_at_time_string = "2020-08-01T08:00:00"
    end_at_datetime = utils.str_datetime_to_datetime(end_at_time_string, utils.DATETIME_STRING_FORMAT)
    end_at = utils.datetime_to_timestamp(end_at_datetime)

    test_collector = "rrc10"

    bcscore_builder = BCScoreBuilder(test_collector, start_at, end_at)
    bcscore_builder.calculate_bcscore()
