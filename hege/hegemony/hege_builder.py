from collections import defaultdict
import json

from hege.bcscore.bcscore_loader import BCSCORELoader
from hege.utils.utils import str_datetime_to_timestamp

bcscore_time_string = "2020-08-01T00:00:00"
bcscore_timestamp = str_datetime_to_timestamp(bcscore_time_string)


class HegeBuilder:
    def __init__(self, collectors: list):
        self.collectors = collectors
        self.hegemony_score_list = defaultdict(list)
        self.hegemony_score = defaultdict(dict)

    def build_hegemony_score(self):
        for collector in self.collectors:
            self.read_data_for_as_hegemony(collector)

    def read_data_for_as_hegemony(self, collector: str):
        loaded_bcscore = BCSCORELoader(collector, bcscore_timestamp).load_data()
        for scope_asn in loaded_bcscore:
            self.hegemony_score_list = defaultdict(list)
            self.load_scoped_average_bcscore_list(loaded_bcscore[scope_asn], scope_asn)
            self.calculate_hegemony(scope_asn)

    def load_scoped_average_bcscore_list(self, depended_asn_bcscore: dict, scope_asn: str):
        for depended_asn in depended_asn_bcscore:

            sum_bcscore_score_in_asn = defaultdict(int)
            peers_count_in_asn = defaultdict(int)
            for peer_asn, peer_bcscore in depended_asn_bcscore[depended_asn]:
                sum_bcscore_score_in_asn[peer_asn] += peer_bcscore
                peers_count_in_asn[peer_asn] += 1

            for peer_asn in sum_bcscore_score_in_asn:
                as_bcscore = sum_bcscore_score_in_asn[peer_asn] / peers_count_in_asn[peer_asn]
                self.hegemony_score_list[depended_asn].append(as_bcscore)

    def calculate_hegemony(self, scope_asn: str):
        total_asn_count = len(self.hegemony_score_list[scope_asn])
        ten_percent = int(total_asn_count*0.1)
        ninety_percent = int(total_asn_count*0.9)
        _range = ninety_percent - ten_percent
        if _range == 0 :
            return

        for asn in self.hegemony_score_list:
            asn_count = len(self.hegemony_score_list[asn])
            score_list = sorted([0]*(total_asn_count-asn_count) + self.hegemony_score_list[asn])
            hege_score = sum(score_list[ten_percent:ninety_percent]) / _range
            if hege_score != 0:
                self.hegemony_score[scope_asn][asn] = hege_score


if __name__ == "__main__":
    test_collectors = ["rrc00", "rrc10"]
    hege_builder = HegeBuilder(test_collectors)
    hege_builder.build_hegemony_score()

    with open("/app/test-hegemony-builder-result.json", "w") as f:
        json.dump(hege_builder.hegemony_score, f, indent=4)
