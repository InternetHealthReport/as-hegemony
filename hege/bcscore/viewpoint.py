from collections import defaultdict
import radix
import json

from hege.bcscore.bgpatom_loader import BGPAtomLoader
import utils


class ViewPoint:
    def __init__(self, ip_address, bgpatom):
        self.peer_address = ip_address
        self.bgpatom = bgpatom
        self.prefixes_weight = radix.Radix()

        self.pow_of_two = [2 ** i for i in range(33)[::-1]]

    def calculate_prefixes_weight(self):
        self.load_ipv4_prefixes()
        node = self.prefixes_weight.add("0.0.0.0/0")
        self.calculate_prefixes_weight_helper(node)

    def calculate_prefixes_weight_helper(self, node):
        sub_prefixes_weight = 0
        prefix = node.prefix

        for sub_node in self.prefixes_weight.search_covered(prefix):
            if sub_node == node:
                continue
            if "weight" not in sub_node.data:
                self.calculate_prefixes_weight_helper(sub_node)
            sub_prefixes_weight += sub_node.data["weight"]

        current_prefix_weight = self.pow_of_two[node.prefixlen] - sub_prefixes_weight
        node.data["weight"] = current_prefix_weight

    def load_ipv4_prefixes(self):
        for aspath in self.bgpatom:
            for prefix, origin_as in self.bgpatom[aspath]:
                self.prefixes_weight.add(prefix)

    def calculate_viewpoint_bcscore(self):
        self.calculate_prefixes_weight()
        bcscore = defaultdict(lambda: defaultdict(int))
        for aspath in self.bgpatom:
            weight_per_asn = self.calculate_accumulated_weight(aspath)
            for origin_asn in weight_per_asn:
                local_graph = bcscore[origin_asn]
                for asn in aspath:
                    local_graph[asn] += weight_per_asn[origin_asn]
                local_graph[origin_asn] += weight_per_asn[origin_asn]
        return self.normalized_bcscore_value(bcscore)

    def calculate_accumulated_weight(self, aspath):
        weight_per_asn = defaultdict(int)
        for prefix, origin_asn in self.bgpatom[aspath]:
            node = self.prefixes_weight.search_exact(prefix)
            weight_per_asn[origin_asn] += node.data["weight"]
        return weight_per_asn

    @staticmethod
    def normalized_bcscore_value(bcscore):
        to_be_removed_asn = list()
        for origin_asn in bcscore:
            origin_asn_total_weight = bcscore[origin_asn][origin_asn]

            if origin_asn_total_weight == 0:
                to_be_removed_asn.append(origin_asn)
            else:
                for asn in bcscore[origin_asn]:
                    bcscore[origin_asn][asn] /= origin_asn_total_weight

        for origin_asn in to_be_removed_asn:
            bcscore.pop(origin_asn)

        return bcscore


def debug__test1(collector: str, timestamp: int):
    collector_bgpatom = BGPAtomLoader(collector, timestamp).load_bgpatom()
    sample_peer_address = list(collector_bgpatom.keys())[0]
    return collector_bgpatom[sample_peer_address]


def debug__test2():
    peer_bgpatom = {
        (1, 2, 3): [("8.8.8.0/24", 1), ("8.8.8.0/25", 2), ("8.8.8.128/25", 3)],
        (2, 3, 4): [("8.8.0.0/16", 4), ("8.9.0.0/16", 5), ("8.8.0.0/17", 6)]
    }
    return peer_bgpatom


if __name__ == "__main__":
    test_collector = "rrc10"
    atom_timestamp = utils.str_datetime_to_timestamp("2020-08-01T00:00:00")

    sample_viewpoint = ViewPoint("1.1.1.1", debug__test1(test_collector, atom_timestamp))
    sample_viewpoint_bcscore = sample_viewpoint.calculate_viewpoint_bcscore()
    with open("bc_score_sample.json", "w") as f:
        json.dump(sample_viewpoint_bcscore, f, indent=4)
