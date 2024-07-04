import json
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from scipy import stats

from hege.bcscore.bcscore_loader import BCSCORELoader
from hege.utils.utils import str_datetime_to_timestamp


class HegeBuilderHelper:
    """Helper class that calculates Hegemony scores for a single timestamp.

    First, read BC scores for the specified collectors and timestamp from Kafka in
    parallel. If there is no data available for a collector, it is removed from the
    list, which is passed by reference. Thus, the collector is excluded from subsequent
    analyses that are part of the same HegeBuilder.consume_and_calculate() call, even
    though new HegeBuilderHelper objects are created.

    The BC scores are combined into one data structure per scope:

        dict[scope] -> dict[on-path ASN] -> dict[peer ASN] -> list[BC score]

    Usually each peer ASN only has a single BC score for each on-path AS per scope
    (i.e., the list has a length of one).  However, there can be cases where multiple BC
    scores from a single peer AS are available (caused by a peer connecting to multiple
    route collectors). In this case, the scores are averaged (like is done in
    BCScoreBuilder.calculate_bcscore_per_asn for peer ASes present with multiple IPs at
    one collector).

    Each peer-scope combination has its own local graph consisting of on-path ASes that
    each have one BC score, which would result in an intuitive mapping of:

        dict[scope] -> dict[peer ASN] -> dict[on-path ASN] -> BC score

    However, the goal is to merge these local graphs, which is why the mapping switches
    the order of peer and on-path ASN. Thus, the on-path ASNs in the first level of the
    map are a union of ASes present in the different local graphs. This also means that
    each on-path ASN only maps to a subset of peers, namely the peers that see this AS
    in their local graph. For the calculation we need a full mesh of on-path ASNs and
    peer ASNs though, i.e., each on-path ASN should have scores for all peer ASNs. Thus,
    if a peer ASN does not have the on-path ASN in its local graph, we add it with a
    score of zero.

    Example:
                (on-path ASN, BC score)
        peer A: (ASX, a_x), (ASY, a_y)
        peer B: (ASY, b_y), (ASZ, b_z)
        -> add (ASZ, 0) to peer A and (ASX, 0) to peer B

    Finally, to get the Hegemony score for an on-path ASN of a single scope, first trim
    the top and bottom x% (default 10%) off the sorted list and then average the
    remaining values.

    There are two different ways to pad the BC score lists. In the normal mode the list
    is extended to the number of all peer ASes seen by any collector independent of the
    scope, i.e., even if a peer AS has no local graph for a scope, the on-path AS will
    get a score of zero from it.  This mode should be used if it is expected that
    roughly all peers will have local graphs for all scopes (e.g., BGP data from route
    collectors).

    The other way is "sparse-peer mode" which pads the list only to the number of peers
    that have a local graph for the scope (like the example above). This mode should be
    used if each peer only has local graphs for a limited number of scopes. In this
    case, the number of zero scores added in the normal mode would dominate the
    Hegemony score and should be avoided.
    """

    def __init__(self, collectors: list, timestamp: int, prefix_mode=False, partition_id=None, sparse_peers=False):
        self.collectors = collectors
        self.timestamp = timestamp
        self.prefix_mode = prefix_mode
        # If specified, only process BC scores for one partition.
        self.partition_id = partition_id

        # Set of all peer ASNs, independent of scope. Used to pad the final BC score
        # list to the length of all peers if not in sparse-peer mode.
        self.peer_asn_set = set()
        # Helper variable which is set to len(peer_asn_set).
        self.total_peer_asn_count = 0

        # Enable sparse-peer mode.
        self.sparse_peers = sparse_peers
        # Set of peer ASNs that have local graphs per scope. This is used instead of the
        # total number of peers if in sparse-peer mode.
        self.peer_asn_set_per_scope = defaultdict(set)

        # Keep track of BC scores.
        #   dict[scope] -> dict[path_asn] -> dict[peer_asn] -> list[bc_score]
        # Populated in read_data_for_as_hegemony()
        self.bc_score_list = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        # The final Hegemony scores.
        #   dict[scope] -> [path_asn] -> {'hege': x, 'nb_peers': y}
        # Populated by calculate_hegemony() and accessed from the outside by
        # HegeBuilder.dump_as_hegemony_score()
        self.hegemony_score = defaultdict(dict)

    def build_hegemony_score(self):
        """Read BC score data from Kafka and calculate Hegemony scores.

        This function alters the collectors list, which will be reflected in the
        function calling HegeBuilderHelper. It removes collectors for which no BC scores
        are available.
        """

        to_remove = []

        # Use multiple thread to fetch data concurrently.
        with ThreadPoolExecutor() as tpool:
            res = tpool.map(self.read_data_for_as_hegemony, self.collectors)
            # Check if everything went fine
            for success, collector in res:
                if not success:
                    to_remove.append(collector)

        # Ignore collectors that were aborted
        for collector in to_remove:
            logging.error(f"IGNORING {collector} for the rest of the analysis")
            self.collectors.remove(collector)

        self.calculate_hegemony()

    def read_data_for_as_hegemony(self, collector: str):
        """Read BC scores for the specified collector from Kafka.

        Args:
            collector (str): The collector for which to read BC scores.

        Returns:
            Tuple[bool, str]: Return True if read was successful and the name of the
            collector.
        """
        loaded_bcscore = self.load_bcscore(collector, self.partition_id)

        if not loaded_bcscore:
            logging.debug(f"could not read collector {collector}'s bcscore;")
            return False, collector

        logging.debug(f"successfully read collector {collector}'s bcscore; {len(loaded_bcscore)} scopes data loaded")
        logging.debug(f"start analyzing {collector}'s bcscore")
        # Sort BC scores into data structure.
        for scope in loaded_bcscore:
            depended_ases_bcscore = loaded_bcscore[scope]
            for depended_as in depended_ases_bcscore:
                for peer_asn, as_bcscore in depended_ases_bcscore[depended_as]:
                    self.peer_asn_set.add(peer_asn)
                    self.peer_asn_set_per_scope[scope].add(peer_asn)
                    # Keep track of scores per peer ASN. Usually there should only be
                    # one score per peer ASN, but some might be present at multiple
                    # collectors, resulting in multiple scores.
                    self.bc_score_list[scope][depended_as][peer_asn].append(as_bcscore)
        logging.debug(f"complete analyzing {collector}'s bcscore")

        return True, collector

    def load_bcscore(self, collector: str, partition_id=None):
        logging.debug(f"read {collector}'s bcscore")
        loaded_bcscore = BCSCORELoader(collector, self.timestamp, self.prefix_mode, partition_id).load_data()
        return loaded_bcscore

    def calculate_hegemony(self):
        """Calculate Hegemony scores in parallel."""
        logging.info("start calculating hegemony score")
        self.total_peer_asn_count = len(self.peer_asn_set)

        logging.info(f"total number of peer asn: {self.total_peer_asn_count}")
        # Calculate scores per scope in parallel.
        with ThreadPoolExecutor() as tpool:
            res = tpool.map(self.calculate_hegemony_helper, self.bc_score_list.items(), chunksize=1000)
            # Needed to log if an exception is raised.
            for r in res:
                pass

        # for scope in self.bc_score_list:
            # self.calculate_hegemony_helper(scope)
        logging.info("complete calculating hegemony score")

    def calculate_hegemony_helper(self, args):
        """Calculate Hegemony scores for a single scope.

        Each on-path AS in the combined local graph of the scope will get a score.

        The process starts with a list of BC scores from different peer ASes. If there
        are multiple scores from one peer ASN for a single on-path ASN, average these
        scores first, so that a single peer only has a single entry in the list.

        Then, pad the list with zero scores for peers that do not see the on-path AS. If
        sparse-peer mode is used, only peers with paths towards the scope are counted,
        else all peers are used.

        Finally, sort the BC score list, trim the top and bottom 10% and calculate the
        average of the remaining values, which is the Hegemony score for the on-path
        ASN.

        Args:
            args (Tuple[str, dict]): Scope and dict[path_asn] -> dict[peer_asn] ->
            list[bc_score]
        """
        # scope_bc_score_list = self.bc_score_list[scope]
        # if self.prefix_mode:
        #     total_asn_count = max([len(scope_bc_score_list[asn]) for asn in scope_bc_score_list])
        # else:
        #    total_asn_count = len(scope_bc_score_list[scope])

        # Calculate hegemony per scope.
        scope, scope_bc_score_list = args

        # By default, pad the list of BC scores to the number of all peers.
        total_nb_peers = self.total_peer_asn_count
        scope_nb_peers = len(self.peer_asn_set_per_scope[scope])
        # In sparse-peer mode, only peers that have at least one path to the scope are
        # used.
        if self.sparse_peers:
            total_nb_peers = scope_nb_peers

        # Iterate over ASes in the local graph of the scope.
        # Each AS has a list of BS scores from different peer ASes.
        for asn, peers_bc_scores in scope_bc_score_list.items():
            flattened_peers_bc_score_list = list()
            # Some peer ASes might be present at multiple collectors, giving multiple BC
            # scores for the same AS. Average these (like is done in
            # BGPAtomBuilder.calculate_bcscore_per_asn for peer ASes with multiple peer
            # IPs at one collector) to prevent skew of the trim_mean.
            for peer_asn, peer_bc_score_list in peers_bc_scores.items():
                if len(peer_bc_score_list) == 1:
                    flattened_peers_bc_score_list.append(peer_bc_score_list[0])
                else:
                    flattened_peers_bc_score_list.append(sum(peer_bc_score_list) / len(peer_bc_score_list))
            # Add 0 for peers that haven't seen this AS.
            peer_asn_count = len(scope_bc_score_list[asn])
            flattened_peers_bc_score_list += [0] * (total_nb_peers - peer_asn_count)
            hege_score = float(stats.trim_mean(flattened_peers_bc_score_list, 0.1))
            if hege_score != 0:
                self.hegemony_score[scope][asn] = {'hege': hege_score, 'nb_peers': scope_nb_peers}


if __name__ == "__main__":
    FORMAT = '%(asctime)s %(name)s %(message)s'
    logging.basicConfig(
        format=FORMAT, level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
    )

    bcscore_time_string = "2020-08-01T00:00:00"
    bcscore_timestamp = str_datetime_to_timestamp(bcscore_time_string)

    test_collectors = ["rrc00", "rrc10", "route-views.linx", "route-views2"]
    hege_builder = HegeBuilderHelper(test_collectors, bcscore_timestamp)
    hege_builder.build_hegemony_score()

    with open("/app/test-as-hegemony-builder-result.json", "w") as f:
        json.dump(hege_builder.hegemony_score, f, indent=4)
