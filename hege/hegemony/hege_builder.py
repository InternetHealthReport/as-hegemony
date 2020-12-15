from collections import defaultdict
import json
import logging

from hege.bcscore.bcscore_loader import BCSCORELoader
from hege.utils.utils import str_datetime_to_timestamp


class HegeBuilder:
    def __init__(self, collectors: list, timestamp: int):
        self.collectors = collectors
        self.timestamp = timestamp
        self.hegemony_score_list = defaultdict(lambda: defaultdict(list))
        self.hegemony_score = defaultdict(dict)

    def build_hegemony_score(self):
        for collector in self.collectors:
            logging.debug(f"read {collector}'s bcscore")
            self.read_data_for_as_hegemony(collector)
        self.calculate_hegemony()

    def read_data_for_as_hegemony(self, collector: str):
        loaded_bcscore = BCSCORELoader(collector, self.timestamp).load_data()
        logging.debug(f"read collector {collector}'s bcscore; {len(loaded_bcscore)} ases data loaded")

        for scope_asn in loaded_bcscore:
            self.load_scoped_average_bcscore_list(loaded_bcscore[scope_asn], scope_asn)

    def load_scoped_average_bcscore_list(self, depended_asn_bcscore: dict, scope_asn: str):
        for depended_asn in depended_asn_bcscore:

            sum_bcscore_score_in_asn = defaultdict(int)
            peers_count_in_asn = defaultdict(int)
            for peer_asn, peer_bcscore in depended_asn_bcscore[depended_asn]:
                sum_bcscore_score_in_asn[peer_asn] += peer_bcscore
                peers_count_in_asn[peer_asn] += 1

            for peer_asn in sum_bcscore_score_in_asn:
                as_bcscore = sum_bcscore_score_in_asn[peer_asn] / peers_count_in_asn[peer_asn]
                if as_bcscore != 0:
                    self.hegemony_score_list[scope_asn][depended_asn].append(as_bcscore)

    def calculate_hegemony(self):
        logging.info("start calculating as hegemony score")
        for i, scope_asn in enumerate(self.hegemony_score_list):
            self.calculate_hegemony_helper(scope_asn)

    def calculate_hegemony_helper(self, scope_asn: str):
        scope_asn_hegemony_score_list = self.hegemony_score_list[scope_asn]
        total_asn_count = len(scope_asn_hegemony_score_list[scope_asn])

        ten_percent = int(total_asn_count*0.1)
        ninety_percent = int(total_asn_count*0.9)
        _range = ninety_percent - ten_percent
        if _range == 0:
            return

        for asn in scope_asn_hegemony_score_list:
            asn_count = len(scope_asn_hegemony_score_list[asn])
            peers_bc_score_list = [0] * (total_asn_count - asn_count) + scope_asn_hegemony_score_list[asn]
            sorted_peers_bc_score_list = sorted(peers_bc_score_list)
            hege_score = sum(sorted_peers_bc_score_list[ten_percent:ninety_percent]) / _range
            if hege_score != 0:
                self.hegemony_score[scope_asn][asn] = hege_score


if __name__ == "__main__":
    FORMAT = '%(asctime)s %(name)s %(message)s'
    logging.basicConfig(
        format=FORMAT, level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
    )

    bcscore_time_string = "2020-08-01T00:00:00"
    bcscore_timestamp = str_datetime_to_timestamp(bcscore_time_string)

    test_collectors = ["rrc00", "rrc10", "route-views.linx", "route-views2"]
    hege_builder = HegeBuilder(test_collectors, bcscore_timestamp)
    hege_builder.build_hegemony_score()

    with open("/app/test-hegemony-builder-result.json", "w") as f:
        json.dump(hege_builder.hegemony_score, f, indent=4)
