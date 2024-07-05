import math
from collections import defaultdict

from hege.bcscore.viewpoint import ViewPoint
from hege.bgpatom.bgpatom_loader import BGPAtomLoader
from hege.utils import utils
from hege.utils.config import Config

DUMP_INTERVAL = Config.get("bcscore")["dump_interval"]
AS_BCSCORE_META_DATA_TOPIC = Config.get("bcscore")["meta_data_topic__as"]
AS_BCSCORE_DATA_TOPIC = Config.get("bcscore")["data_topic__as"]
PREFIX_BCSCORE_META_DATA_TOPIC = Config.get("bcscore")["meta_data_topic__prefix"]
PREFIX_BCSCORE_DATA_TOPIC = Config.get("bcscore")["data_topic__prefix"]

DATA_BATCH_SIZE = 10000


class BCScoreBuilder:
    """Calculate BC scores from BGP Atoms.

    This class reads the BGP atom topic of the specified collector from Kafka for the
    specified timeframe. Atoms are processed in DUMP_INTERVAL steps.

    First create BC scores per peer IP. Then, merge the scores per peer ASN by averaging
    in case an AS has multiple peer IPs for this collector. Create output messages per
    peer ASN and scope (ASN or prefix, depending on the mode).

    For more info on the BC score calculation, check the ViewPoint class.
    """

    def __init__(self, collector: str, start_timestamp: int, end_timestamp: int,
                 prefix_mode=False, address_family=4):
        self.collector = collector
        # Round the start time up to the next DUMP_INTERVAL.
        self.start_timestamp = math.ceil(start_timestamp/DUMP_INTERVAL) * DUMP_INTERVAL
        self.end_timestamp = end_timestamp
        self.prefix_mode = prefix_mode
        self.address_family = address_family

        if prefix_mode:
            self.kafka_data_topic = f"{PREFIX_BCSCORE_DATA_TOPIC}_{collector}"
            self.kafka_meta_data_topic = f"{PREFIX_BCSCORE_META_DATA_TOPIC}_{collector}"
        else:
            self.kafka_data_topic = f"{AS_BCSCORE_DATA_TOPIC}_{collector}"
            self.kafka_meta_data_topic = f"{AS_BCSCORE_META_DATA_TOPIC}_{collector}"

    def consume_and_calculate(self):
        """Read the BGP atom topic for this collector and produce BC score messages in
        DUMP_INTERVAL steps.

        Yields:
            Tuple[int, Generator]: The timestamp of the current dump and a Generator for
            the BC score Kafka messages.
        """
        # Process the entire timeframe in DUMP_INTERVAL steps.
        for current_timestamp in range(self.start_timestamp, self.end_timestamp, DUMP_INTERVAL):
            bgpatom = self.load_bgpatom(current_timestamp)
            yield current_timestamp, self.get_viewpoint_bcscore_generator(bgpatom, current_timestamp)

    def load_bgpatom(self, atom_timestamp):
        bgpatom = BGPAtomLoader(self.collector, atom_timestamp).load_data()
        return bgpatom

    def get_viewpoint_bcscore_generator(self, bgpatom: dict, atom_timestamp: int):
        """Generate BC score messages per scope and peer ASN.

        Args:
            bgpatom (dict): Dictionary containing BGP atoms per peer IP.
            atom_timestamp (int): Unused

        Yields:
            Tuple[dict, str]: Formatted Kafka message and scope. Message contains (part
            of) the BC scores from a single peer ASN to the scope.
        """
        # Group the peer IPs by the peer ASN so that we can iterate by ASN.
        peers_by_asn = defaultdict(list)
        for peer_address, peer_asn in bgpatom:
            peers_by_asn[peer_asn].append(peer_address)

        # Iterate by peer ASN. Pass the entire BGP atom structure to
        # calculate_bcscore_per_asn, as it will extract the required atoms.
        for peer_asn in peers_by_asn:
            peers_list = peers_by_asn[peer_asn]
            for message, scope in self.calculate_bcscore_per_asn(peers_list, peer_asn, bgpatom, atom_timestamp):
                yield message, scope

    def calculate_bcscore_per_asn(self, peers_in_asn_list: list, peer_asn: str, bgpatom: dict, atom_timestamp: int):
        """Calculate BC scores for a single peer ASN.

        First, calculate BC scores per scope per peer IP. If there are multiple peer
        IPs, merge the BC scores per scope by averaging the scores.

        Args:
            peers_in_asn_list (list): IPs belonging to this peer.
            peer_asn (str): Peer ASN.
            bgpatom (dict): Dict containing BGP atoms for all peers.
            atom_timestamp (int): Unused.

        Yields:
            dict: Kafka message.
        """
        # Sum of BC scores per scope -> path ASN.
        sum_bcscore = defaultdict(lambda: defaultdict(int))
        # Number of peer IPs per scope -> path ASN.
        peers_count = defaultdict(lambda: defaultdict(int))

        # Calculate BC scores per peer IP.
        for peer_address in peers_in_asn_list:
            # Skip peers for wrong IP version
            if (('.' in peer_address and self.address_family == 6)
                    or (':' in peer_address and self.address_family == 4)):
                continue

            peer_bgpatom = bgpatom[(peer_address, peer_asn)]
            # Calculate BC scores for this peer IP. Scores are generated per scope.
            peer_bcscore_generator = self.calculate_viewpoint_bcscore(peer_bgpatom, peer_address, atom_timestamp)
            # Aggregate scores per scope for averaging later.
            for scope_bcscore, scope in peer_bcscore_generator:
                for depended_asn, depended_asn_bcscore in scope_bcscore.items():
                    sum_bcscore[scope][depended_asn] += depended_asn_bcscore
                    peers_count[scope][depended_asn] += 1

        # Yield scores per scope.
        for scope in sum_bcscore:
            # Average scores. If the peer ASN is only connected with one IP to the
            # connector, this action does nothing.
            bcscore_by_asn = dict()
            for depended_asn in sum_bcscore[scope]:
                bcscore_by_asn[depended_asn] = sum_bcscore[scope][depended_asn] / peers_count[scope][depended_asn]

            # Limit the number of asn (message size). Dump BC scores in batches of
            # DATA_BATCH_SIZE.
            bba_list = list(bcscore_by_asn.items())
            idx = range(len(bba_list))
            for bba_batch in idx[::DATA_BATCH_SIZE]:
                yield (self.format_dump_data(dict(bba_list[bba_batch:bba_batch+DATA_BATCH_SIZE]),
                                             scope,
                                             peer_asn,
                                             atom_timestamp),
                       scope)

    def calculate_viewpoint_bcscore(self, bgpatom: dict, peer_address: str, atom_timestamp: int):
        """Calculate BC scores for a single peer IP.

        Args:
            bgpatom (dict): Dictionary containing BGP atoms for this peer IP.
            peer_address (str): Peer IP. Used to determine address version.
            atom_timestamp (int): Unused.

        Returns:
            Generator[dict, str]: Per-scope BC scores and scope.
        """
        viewpoint = ViewPoint(peer_address, self.collector, bgpatom, atom_timestamp, self.prefix_mode)
        return viewpoint.calculate_viewpoint_bcscore()

    @staticmethod
    def format_dump_data(bcscore_by_asn: dict, scope: str, peer_asn: str, dump_timestamp: int):
        return {
            "bcscore": bcscore_by_asn,
            "scope": scope,
            "peer_asn": peer_asn,
            "timestamp": dump_timestamp
        }


if __name__ == "__main__":
    start_at_time_string = "2020-08-01T00:00:00"
    start_at = utils.str_datetime_to_timestamp(start_at_time_string)

    end_at_time_string = "2020-08-01T00:01:00"
    end_at = utils.str_datetime_to_timestamp(end_at_time_string)

    test_collector = "rrc10"

    bcscore_builder = BCScoreBuilder(test_collector, start_at, end_at)
    for timestamp, viewpoint_bcscore_generator in bcscore_builder.consume_and_calculate():
        print(timestamp)
        for bcscore, test_scope in viewpoint_bcscore_generator:
            print(test_scope, bcscore)
            break
        break
