from collections import defaultdict
import json

from hege.bgpatom.bgpatom_peer import BGPAtomPeer
import bgpdata
import utils


with open("config.json", "r") as f:
    config = json.load(f)
DUMP_INTERVAL = config["bgpatom"]["dump_interval"]
FULL_FLEET_THRESHOLD = config["bgpatom"]["full_fleet_threshold"]


class BGPAtomBuilder:
    def __init__(self, collector, start_timestamp: int, end_timestamp: int):
        self.collector = collector
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp

        self.bgpatom_peers = dict()

    def get_bgpatom_peer(self, peer_address: str):
        if peer_address not in self.bgpatom_peers:
            self.set_bgpatom_peer(peer_address)
        return self.bgpatom_peers[peer_address]

    def set_bgpatom_peer(self, peer_address: str):
        self.bgpatom_peers[peer_address] = BGPAtomPeer(peer_address)

    def read_bgp_message_and_construct_atom(self):
        next_dumped_timestamp = self.start_timestamp
        for element in bgpdata.consume_ribs_and_update_message_upto(
                self.collector, self.start_timestamp, self.end_timestamp):

            if element["time"] > next_dumped_timestamp:
                yield next_dumped_timestamp
                next_dumped_timestamp += DUMP_INTERVAL

            peer_address = element["peer_address"]
            bgpatom_peer = self.get_bgpatom_peer(peer_address)
            bgpatom_peer.update_prefix_status(element)

    def dump_bgpatom_messages(self, timestamp: int, full_fleet_threshold: int):
        for peer_address in self.bgpatom_peers:
            bgpatom_peer = self.bgpatom_peers[peer_address]

            if bgpatom_peer.is_full_fleet(full_fleet_threshold):
                for bgpatom_kafka_message in bgpatom_peer.dump_bgpatom(timestamp):
                    yield bgpatom_kafka_message


if __name__ == "__main__":
    test_collector = "rrc10"

    start_time_string = "2020-08-01T00:00:00"
    start_datetime = utils.str2dt(start_time_string, utils.DATETIME_STRING_FORMAT)
    start_ts = utils.dt2ts(start_datetime)

    end_time_string = "2020-08-01T00:16:00"
    end_datetime = utils.str2dt(end_time_string, utils.DATETIME_STRING_FORMAT)
    end_ts = utils.dt2ts(end_datetime)

    bgpatom_builder = BGPAtomBuilder(test_collector, start_ts, end_ts)
    for ts in bgpatom_builder.read_bgp_message_and_construct_atom():
        for message in bgpatom_builder.dump_bgpatom_messages(ts, FULL_FLEET_THRESHOLD):
            print(ts, message)
            break
