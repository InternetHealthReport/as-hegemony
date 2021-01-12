from collections import defaultdict
import json
import logging

from hege.bgpatom.bgpatom_loader import BGPAtomLoader
from hege.utils.utils import str_datetime_to_timestamp


class HegeBuilderPrefix:
    def __init__(self, collectors: list, timestamp: int):
        self.collectors = collectors
        self.timestamp = timestamp

        self.prefixes_announced_by_peers = defaultdict(int)
        self._hegemony_score = defaultdict(lambda: defaultdict(int))
        self.hegemony_score = dict()

    def build_hegemony_score(self):
        for collector in self.collectors:
            self.build_hegemony_score_helper(collector)
        logging.debug(f"successfully load all atoms from [{', '.join(self.collectors)}]")
        return self.calculate_and_yield_hegemony()

    def build_hegemony_score_helper(self, collector: str):
        loaded_bgpatom = BGPAtomLoader(collector, self.timestamp).load_data()
        logging.debug(f"read collector {collector}'s bgpatom; {len(loaded_bgpatom)} peer's atom loaded")

        total_peers = len(loaded_bgpatom)
        for peer_id, peer_ip in enumerate(loaded_bgpatom):
            logging.debug(f"{peer_id+1}/{total_peers}: read peer {peer_ip}'s bgpatom; {len(peer_ip)} atom loaded")
            self.read_peer_atom(loaded_bgpatom[peer_ip])

    def read_peer_atom(self, peer_atom):
        for aspath in peer_atom:
            for prefix, origin_asn in peer_atom[aspath]:
                self.prefixes_announced_by_peers[prefix] += 1
                self._hegemony_score[prefix][origin_asn] += 1
                for asn in aspath:
                    self._hegemony_score[prefix][asn] += 1

    def calculate_and_yield_hegemony(self):
        total_prefix = len(self.prefixes_announced_by_peers)
        logging.debug(f"start calculating prefix hegemony for {total_prefix} prefix(es)")

        for prefix in self.prefixes_announced_by_peers:
            total_seen_count = self.prefixes_announced_by_peers[prefix]
            lower_bound = int(0.1 * total_seen_count)
            upper_bound = int(0.9 * total_seen_count)
            bounded_length = upper_bound - lower_bound
            if bounded_length == 0:
                continue
            result = dict()
            for asn in self._hegemony_score[prefix]:
                sum_bcscore = self._hegemony_score[prefix][asn]
                if sum_bcscore > upper_bound:
                    result[asn] = 1
                elif sum_bcscore <= lower_bound:
                    continue
                else:
                    result[asn] = (sum_bcscore - lower_bound)/bounded_length
            self.hegemony_score[prefix] = result

        logging.debug(f"completed calculating all prefixes hegemony")
        return


if __name__ == "__main__":
    FORMAT = '%(asctime)s %(name)s %(message)s'
    logging.basicConfig(
        format=FORMAT, level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
    )

    prefix_hege_time_string = "2020-08-01T00:00:00"
    prefix_hege_timestamp = str_datetime_to_timestamp(prefix_hege_time_string)

    test_collectors = ["rrc00", "rrc10", "route-views.linx", "route-views2"]
    # test_collectors = ["rrc10"]
    hege_builder = HegeBuilderPrefix(test_collectors, prefix_hege_timestamp)

    test_result = dict()
    for res_prefix, hegemony in hege_builder.build_hegemony_score():
        test_result[res_prefix] = hegemony

    with open("/app/test-prefix-hegemony-builder-result.json", "w") as f:
        json.dump(test_result, f, indent=4)
