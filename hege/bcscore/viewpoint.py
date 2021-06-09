from collections import defaultdict, Counter
import radix
import json
import logging

from hege.utils import utils
SCOPE_ASN = "-1"


def is_set_as(asn: str):
    return asn[0] == "{"


def get_asn_set(asn: str):
    return asn[1: -1].split(",")


class ViewPoint:
    def __init__(self, ip_address: str, collector: str, bgpatom: dict, timestamp: int, 
            prefix_mode=False):
        self.peer_address = ip_address
        self.collector = collector
        self.bgpatom = bgpatom
        self.timestamp = timestamp
        if '.' in self.peer_address:
            self.address_family = 4
        else:
            self.address_family = 6

        self.peer_asn = None

        self.prefix_mode = prefix_mode
        if prefix_mode:
            pass
        else:
            self.prefixes_weight = radix.Radix()
            self.pow_of_two = [2 ** i for i in range(33)[::-1]]

    def calculate_prefixes_weight(self):
        self.load_prefixes()
        if self.address_family == 4:
            node = self.prefixes_weight.add("0.0.0.0/0")
        else:
            node = self.prefixes_weight.add("::/0")
        self.calculate_prefixes_weight_helper(node)

    def calculate_prefixes_weight_helper(self, node):
        sub_prefixes_weight = 0
        prefix = node.prefix

        if self.address_family == 4 and '.' in prefix:
            for sub_node in self.prefixes_weight.search_covered(prefix):
                if sub_node == node:
                    continue
                if "weight" not in sub_node.data:
                    self.calculate_prefixes_weight_helper(sub_node)
                sub_prefixes_weight += sub_node.data["weight"]

            current_prefix_weight = self.pow_of_two[node.prefixlen] - sub_prefixes_weight
            node.data["weight"] = current_prefix_weight
        elif self.address_family == 6 and ':' in prefix:
            # For IPv6 all paths have equal weight
            for sub_node in self.prefixes_weight.search_covered(prefix):
                if sub_node == node:
                    continue
                if "weight" not in sub_node.data:
                    self.calculate_prefixes_weight_helper(sub_node)

            node.data["weight"] = 1


    def load_prefixes(self):
        for aspath in self.bgpatom:
            for prefix, _ in self.bgpatom[aspath]:
                if self.address_family == 6 and  utils.is_ip_v6(prefix):
                    self.prefixes_weight.add(prefix)
                elif self.address_family == 4 and not utils.is_ip_v6(prefix): 
                    self.prefixes_weight.add(prefix)

    def calculate_viewpoint_bcscore(self):
        logging.debug(f"start calculating bcscore ({self.peer_address})")
        if self.prefix_mode:
            return self.__calculate_viewpoint_bcscore_for_prefix()
        else:
            return self.__calculate_viewpoint_bcscore_for_asn()

    def __calculate_viewpoint_bcscore_for_prefix(self):
        bcscore = defaultdict(lambda: defaultdict(int))

        for aspath in self.bgpatom:
            for prefix, origin_asn in self.bgpatom[aspath]:
                # scope for prefixes include the origin ASN
                self.set_asn_weight(origin_asn, 1, bcscore[prefix+'_'+origin_asn])
                for asn in aspath:
                    self.set_asn_weight(asn, 1, bcscore[prefix+'_'+origin_asn])

        for prefix, bcs in bcscore.items():
            yield bcs, prefix

    def __calculate_viewpoint_bcscore_for_asn(self):
        self.calculate_prefixes_weight()

        bcscore = defaultdict(lambda: defaultdict(int))
        for aspath in self.bgpatom:
            weight_per_asn = self.calculate_accumulated_weight(aspath)
            for origin_asn in weight_per_asn:
                local_graph = bcscore[origin_asn]
                asn_weight = weight_per_asn[origin_asn]
                for asn in aspath:
                    self.set_asn_weight(asn, asn_weight, local_graph)

                local_graph[origin_asn] += weight_per_asn[origin_asn]

                # patch
                if origin_asn != SCOPE_ASN:
                    bcscore[SCOPE_ASN][origin_asn] += weight_per_asn[origin_asn]

        normalized_bcscore = self.normalized_bcscore_value(bcscore)
        for origin_asn in normalized_bcscore:
            yield normalized_bcscore[origin_asn], origin_asn

    def calculate_accumulated_weight(self, aspath):
        weight_per_asn = defaultdict(int)
        for prefix, origin_asn in self.bgpatom[aspath]:
            if (    ( self.address_family == 4 and '.' in prefix ) or
                    ( self.address_family == 6 and ':' in prefix ) ):
                    
                node = self.prefixes_weight.search_exact(prefix)
                node_weight = node.data["weight"]
                weight_per_asn[SCOPE_ASN] += node_weight
                self.set_asn_weight(origin_asn, node_weight, weight_per_asn)

        return weight_per_asn

    @staticmethod
    def set_asn_weight(asn: str, weight: int, target):
        if is_set_as(asn):
            asn_set = get_asn_set(asn)
            asn_set_length = len(asn_set)
            for _asn in asn_set:
                asn = _asn.strip()
                target[asn] += weight / asn_set_length
        else:
            target[asn] += weight

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


if __name__ == "__main__":
    test_collector = "rrc10"
    atom_timestamp = utils.str_datetime_to_timestamp("2020-08-01T00:00:00")

    from hege.bgpatom.bgpatom_loader import BGPAtomLoader
    test_bgpatom = BGPAtomLoader(test_collector, atom_timestamp).load_data()
    for peer_address in test_bgpatom:
        peer_atom = test_bgpatom[peer_address]

    sample_viewpoint = ViewPoint("1.1.1.1", test_collector, peer_atom, atom_timestamp, True)
    messages_count = 0
    sample_viewpoint_bcscore = dict()
    for data in sample_viewpoint.calculate_viewpoint_bcscore():
        sample_viewpoint_bcscore[messages_count] = data
        messages_count += 1

    with open("bc_score_sample.json", "w") as f:
        json.dump(sample_viewpoint_bcscore, f, indent=4)
