import json
import logging
from collections import defaultdict

import radix

from hege.utils import utils

# Placeholder that represents the global graph.
GLOBAL_SCOPE = "-1"


def is_as_set(asn: str):
    # AS sets have format {A,B}
    return asn[0] == "{"


def get_asn_set(asn: str):
    return asn[1: -1].split(",")


class ViewPoint:
    """Calculate BC scores for a single peer IP.

    Scores are calculated per scope. The ASes of all atoms leading to the same scope are
    combined in what is called the "local graph" of the scope. Each AS in the graph gets
    a BC score. In addition, we combine all local graphs and scores into one graph
    called the "global graph", which is indicated by a placeholder scope of -1.

    The scope can be an AS or a prefix, depending on the mode. In case of prefix scopes
    there is only one atom per scope (AS path from this peer IP to the prefix) and thus
    all ASes have the same score. If the scope is an AS, then there can be multiple
    atoms for different prefixes originated by the AS. In this case the ASes of each
    atom receive a weighted score depending on the prefixes they connect.

    The weighting process differs between IPv4 and IPv6. For IPv6 each prefix has the
    same weight, e.g., if one atom is used to reach two prefixes, it gets a weight of
    two. For IPv4 the weight depends on the prefix length. For example, an atom
    connecting a /24 prefix gets a weight of 2**(32 - 24) = 256.

    However, for IPv4 this process also takes overlapping prefixes into account to avoid
    double counting. For example, if atom A is used to reach prefix x with a length of
    /23 and atom B for prefix y with /24 and x covers y, then the weight of A is reduced
    by the weight of y to account for the overlap. In this example:

        weight(B) = weight(y) = 2**(32 - 24) = 256

        weight(A) = weight(x) - weight(y) = 2**(32 - 23) - 256 = 256

    The weights are then normalized to a range (0,1] which represent the final BC
    scores. The scope always has a weight of 1 since it is present in all paths even
    though it is technically not part of any atom.
    """
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

        self.prefix_mode = prefix_mode
        if prefix_mode:
            pass
        else:
            # Radix tree containing all prefixes reachable by this peer IP (after
            # initialization by load_prefixes()).
            self.prefixes_weight = radix.Radix()
            # Pre-calculate number of IPs for all prefix lengths for easier access.
            # Index of the list corresponds to prefix length, e.g., pow_of_two[32] = 1.
            self.pow_of_two = [2 ** i for i in range(33)[::-1]]

    def calculate_prefixes_weight(self):
        """Entry for recursive prefix weight calculation.

        First, load all prefixes reachable by this peer IP into a radix tree, then
        initialize root of the tree and start recursion.
        """
        self.load_prefixes()
        if self.address_family == 4:
            node = self.prefixes_weight.add("0.0.0.0/0")
        else:
            node = self.prefixes_weight.add("::/0")
        self.calculate_prefixes_weight_helper(node)

    def calculate_prefixes_weight_helper(self, node):
        """Calculate weight of the specified node and recurse covered prefixes.

        For IPv6 all nodes have the same weight of one. For IPv4 the base weight depends
        on the prefix length and is decremented by the weight of covered prefixes.
        Therefore, first recurse all covered prefixes, sum up their weights and
        subtract it from the base weight of this node.

        Args:
            node (RadixNode): Node representing a prefix.
        """
        sub_prefixes_weight = 0
        prefix = node.prefix

        if self.address_family == 4 and '.' in prefix:
            # Aggregate weight of covered prefixes to later subtract from the weight of
            # this node. If this node is a leaf, there is no recursion.
            for sub_node in self.prefixes_weight.search_covered(prefix):
                # Recurse all covered prefixes and calculate their weight if there were
                # not recursed before.
                if sub_node == node:
                    # A prefix always covers itself, so ignore this case.
                    continue
                if "weight" not in sub_node.data:
                    # Calculate weight of covered prefix.
                    self.calculate_prefixes_weight_helper(sub_node)
                sub_prefixes_weight += sub_node.data["weight"]

            # The weight of the node is the number of IP addresses contained in the
            # prefix (using lookup in the power-of-two table) decremented by the number
            # of IPs covered by smaller prefixes.
            current_prefix_weight = self.pow_of_two[node.prefixlen] - sub_prefixes_weight
            node.data["weight"] = current_prefix_weight
        elif self.address_family == 6 and ':' in prefix:
            # For IPv6 all prefixes have equal weight of one.
            for sub_node in self.prefixes_weight.search_covered(prefix):
                # Ensure all nodes are recursed.
                if sub_node == node:
                    continue
                if "weight" not in sub_node.data:
                    self.calculate_prefixes_weight_helper(sub_node)

            node.data["weight"] = 1

    def load_prefixes(self):
        """Populate self.prefixes_weight radix tree with all prefixes reachable by this
        peer IP.
        """
        for aspath in self.bgpatom:
            for prefix, _ in self.bgpatom[aspath]:
                if self.address_family == 6 and utils.is_ip_v6(prefix):
                    self.prefixes_weight.add(prefix)
                elif self.address_family == 4 and not utils.is_ip_v6(prefix):
                    self.prefixes_weight.add(prefix)

    def calculate_viewpoint_bcscore(self):
        """Calculate BC scores for this peer IP.

        This is a helper function that calls the correct function depending on the mode.

        Returns:
            Generator[dict, str]: Dictionary of ASN to BC score and scope.
        """
        logging.debug(f"start calculating bcscore ({self.peer_address})")
        if self.prefix_mode:
            return self.__calculate_viewpoint_bcscore_for_prefix()
        else:
            return self.__calculate_viewpoint_bcscore_for_asn()

    def __calculate_viewpoint_bcscore_for_prefix(self):
        """Produce BC scores for prefix mode.

        There is no actual calculation in prefix mode since there can only be one atom
        per scope (=prefix), thus all scores are equal to one.

        To account for multi-origin prefixes, the returned scope is not only identified
        by the prefix, but encoded as a combination of prefix and origin ASN. This way
        we later do not accidentally merge local graphs of a prefix that is originated
        by multiple ASes.

        Yields:
            Tuple[dict, str]: Dictionary of ASN to BC score and scope (prefix + origin
            ASN).
        """
        # Map scope -> ASN -> BC score.
        bcscore = defaultdict(lambda: defaultdict(int))

        for aspath in self.bgpatom:
            for prefix, origin_asn in self.bgpatom[aspath]:
                # Scope for prefixes include the origin ASN to account for multi-origin
                # prefixes.
                # Add the origin ASN as well as all on-path ASNs to the local graph with
                # a score of 1.
                self.set_asn_weight(origin_asn, 1, bcscore[prefix+'_'+origin_asn])
                for asn in aspath:
                    self.set_asn_weight(asn, 1, bcscore[prefix+'_'+origin_asn])

        for prefix, bcs in bcscore.items():
            yield bcs, prefix

    def __calculate_viewpoint_bcscore_for_asn(self):
        """Calculate BC scores for AS mode.

        First, calculate weights for all prefixes. Then iterate over all atoms. For each
        atom get the origin ASes that it reaches and add the on-path ASes of the atom to
        the local graphs of all origin ASes. All on-path ASes receive a weight depending
        on the prefix weights they connect.

        Thus, if an AS is part of multiple atoms that reach the same origin AS, it
        receives an aggregated weight.

        Example:

               +- B <-+
               |      |            192.0.2.0/24 via B A X (atom B A)
        Peer <-+      +- A <- X
               |      |         198.51.100.0/24 via C A X (atom C A)
               +- C <-+

        In this case
            weight(B) = weight(C) = 2**(32 - 24) = 256
        but
            weight(A) = 2**(32 - 24) * 2 = 512
        because A is present in two atoms.

        Yields:
            Tuple[dict, str]: Dictionary of ASN to BC score and scope (origin ASN or
            GLOBAL_SCOPE).
        """
        self.calculate_prefixes_weight()

        # Map scope -> ASN -> BC score.
        bcscore = defaultdict(lambda: defaultdict(int))
        for aspath in self.bgpatom:
            # Calculate the weight of each scope (=origin ASN).
            weight_per_origin_asn = self.calculate_accumulated_weight(aspath)
            # Add all on-path ASes to the local graph of each scope reached via this
            # path with the corresponding weight (or increase the weight if an AS is
            # already present in the graph).
            for origin_asn in weight_per_origin_asn:
                # Get the local graph of this scope (defaultdict returns an empty dict
                # if this is the first time we access this graph).
                local_graph = bcscore[origin_asn]
                # Add the on-path ASes to the local graph with the corresponding weight
                # or update the weight if an AS is already present.
                asn_weight = weight_per_origin_asn[origin_asn]
                for asn in aspath:
                    self.set_asn_weight(asn, asn_weight, local_graph)

                # Also add the weight to the origin ASN, which is used as the base for
                # normalization.
                local_graph[origin_asn] += weight_per_origin_asn[origin_asn]

                # The BGP atom does not contain the origin ASN so we have to manually
                # add it to the global graph. However, origin_asn can be GLOBAL_SCOPE in
                # which case the line above already adds the weight and we must not add
                # it twice.
                #   local_graph = bcscore[origin_asn] so
                #   local_graph[origin_asn] == bcscore[origin_asn][origin_asn]
                if origin_asn != GLOBAL_SCOPE:
                    bcscore[GLOBAL_SCOPE][origin_asn] += weight_per_origin_asn[origin_asn]

        normalized_bcscore = self.normalized_bcscore_value(bcscore)
        for origin_asn in normalized_bcscore:
            yield normalized_bcscore[origin_asn], origin_asn

    def calculate_accumulated_weight(self, aspath):
        """Calculate weight per origin AS reachable by this AS path.

        The weight of an origin AS is the sum of weights of all its originated prefixes
        reachable via this AS path.

        Returned dictionary also contains an entry for GLOBAL_SCOPE, which aggregates
        the weight of all prefixes reachable via this AS path independent of the origin
        ASN.

        Args:
            aspath (tuple): AS path

        Returns:
            dict: Dictionary of origin ASN to weight.
        """
        weight_per_asn = defaultdict(int)
        for prefix, origin_asn in self.bgpatom[aspath]:
            if ((self.address_family == 4 and '.' in prefix) or
                    (self.address_family == 6 and ':' in prefix)):

                node = self.prefixes_weight.search_exact(prefix)
                node_weight = node.data["weight"]
                # Could use set_asn_weight for this, but GLOBAL_SCOPE is never a set so
                # no need to check.
                weight_per_asn[GLOBAL_SCOPE] += node_weight
                self.set_asn_weight(origin_asn, node_weight, weight_per_asn)

        return weight_per_asn

    @staticmethod
    def set_asn_weight(asn: str, weight: int, target: dict):
        """Add the weight to the ASN entry in the target dictionary.

        Create an entry for the ASN if it does not exist.

        If asn is an AS set, distribute the weight equally over all ASNs in the set.

        Args:
            asn (str): ASN or AS set to which add weight.
            weight (int): Weight to add.
            target (dict): Dictionary mapping ASN to weight which is updated.
        """
        if is_as_set(asn):
            asn_set = get_asn_set(asn)
            asn_set_length = len(asn_set)
            for _asn in asn_set:
                asn = _asn.strip()
                target[asn] += weight / asn_set_length
        else:
            target[asn] += weight

    @staticmethod
    def normalized_bcscore_value(bcscore):
        """Normalize the BC scores for each graph based on the total weight of the
        origin ASN.

        Args:
            bcscore (dict): Dictionary mapping origin ASN -> on-path ASN -> weight.

        Returns:
            dict: Dictionary mapping origin ASN -> on-path ASN -> normalized BC score.
        """
        to_be_removed_asn = list()
        for origin_asn in bcscore:
            # The origin ASN is in all paths so it has the maximum possible weight.
            origin_asn_total_weight = bcscore[origin_asn][origin_asn]

            # TODO Can this happen?
            if origin_asn_total_weight == 0:
                to_be_removed_asn.append(origin_asn)
            else:
                # Normalize the scores of on-path ASes.
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
