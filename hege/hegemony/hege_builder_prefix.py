from collections import defaultdict
import json
import logging

from hege.bgpatom.bgpatom_loader import BGPAtomLoader
from hege.utils.utils import str_datetime_to_timestamp


def is_set_asn(asn):
    return asn[0] == "{"


class HegeBuilderPrefix:
    def __init__(self, collectors: list, timestamp: int, is_fast_mode=True):
        self.collectors = collectors
        self.timestamp = timestamp

        self.prefixes_announced_by_peers = defaultdict(int)
        self.is_fast_mode = is_fast_mode
        self.hegemony_score = dict()

        if is_fast_mode:
            self._hegemony_score = defaultdict(lambda: defaultdict(int))
        else:
            self._hegemony_score = defaultdict(lambda: defaultdict(list))

    def build_hegemony_score(self):
        for collector in self.collectors:
            loaded_bgpatom = BGPAtomLoader(collector, self.timestamp).load_data()
            logging.debug(f"read collector {collector}'s bgpatom; {len(loaded_bgpatom)} peer's atom loaded")
            self.build_hegemony_score_helper(loaded_bgpatom)

        logging.debug(f"successfully load all atoms from [{', '.join(self.collectors)}]")

        total_prefix = len(self.prefixes_announced_by_peers)
        logging.debug(f"start calculating prefix hegemony for {total_prefix} prefix(es)")
        calculate_and_yield_hegemony_function = self.calculate_and_yield_hegemony_fast if self.is_fast_mode \
            else self.calculate_and_yield_hegemony
        return calculate_and_yield_hegemony_function()

    def build_hegemony_score_helper(self, loaded_bgpatom):
        total_peers = len(loaded_bgpatom)
        read_peer_function = self.read_peer_atom_fast if self.is_fast_mode else self.read_peer_atom
        for peer_id, peer_ip in enumerate(loaded_bgpatom):
            atoms_count = len(loaded_bgpatom[peer_ip])
            progression = f"{peer_id+1}/{total_peers}"
            logging.debug(f"{progression}: read peer {peer_ip}'s bgpatom; {atoms_count} atoms loaded")
            read_peer_function(loaded_bgpatom[peer_ip])

    def read_peer_atom_fast(self, peer_atom):
        for aspath in peer_atom:
            for prefix, origin_asn in peer_atom[aspath]:
                if is_set_asn(origin_asn) and "," not in origin_asn:
                    origin_asn = origin_asn[1:-1]

                self.prefixes_announced_by_peers[prefix] += 1
                self._hegemony_score[prefix][origin_asn] += 1
                for asn in aspath:
                    self._hegemony_score[prefix][asn] += 1

    def read_peer_atom(self, peer_atom):
        for aspath in peer_atom:
            for prefix, origin_asn in peer_atom[aspath]:
                self.prefixes_announced_by_peers[prefix] += 1
                if is_set_asn(origin_asn):
                    origin_list = origin_asn[1:-1].split(",")
                    as_score = 1 / len(origin_list)
                else:
                    origin_list = [origin_asn]
                    as_score = 1

                for origin in origin_list:
                    self._hegemony_score[prefix][origin].append(as_score)
                    for asn in aspath:
                        if is_set_asn(asn):
                            members_asn_list = asn[1:-1].split(",")
                            members_asn_count = len(members_asn_list)
                            member_score = 1 / members_asn_count
                            for member in members_asn_list:
                                self._hegemony_score[prefix][member].append(member_score)
                        else:
                            self._hegemony_score[prefix][asn].append(1)

    def calculate_and_yield_hegemony_fast(self):
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

    def calculate_and_yield_hegemony(self):
        for prefix in self.prefixes_announced_by_peers:
            total_seen_count = self.prefixes_announced_by_peers[prefix]

            ten_percent = int(total_seen_count * 0.1)
            ninety_percent = int(total_seen_count * 0.9)
            bounded_length = ninety_percent - ten_percent
            if bounded_length == 0:
                continue

            result = dict()
            for asn in self._hegemony_score[prefix]:
                score_list_length = len(self._hegemony_score[prefix][asn])
                score_list = [0] * (total_seen_count - score_list_length) + self._hegemony_score[prefix][asn]
                score_list.sort()

                hege_score = sum(score_list[ten_percent:ninety_percent]) / bounded_length
                if hege_score:
                    result[asn] = hege_score

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
    hege_builder = HegeBuilderPrefix(test_collectors, prefix_hege_timestamp)

    test_result = dict()
    for res_prefix, hegemony in hege_builder.build_hegemony_score():
        test_result[res_prefix] = hegemony

    with open("/app/test-prefix-hegemony-builder-result.json", "w") as f:
        json.dump(test_result, f, indent=4)
