import logging
from collections import defaultdict

from hege.utils import utils
from hege.utils.config import Config

WITHDRAWN_PATH_ID = -1
PREFIXES_IN_ATOM_BATCH_SIZE = Config.get("bgpatom")["prefixes_in_atom_batch_size"]
FULL_FEED_PREFIXES_THRESHOLD_v4 = Config.get("bgpatom")["full_feed_threshold_v4"]
FULL_FEED_PREFIXES_THRESHOLD_v6 = Config.get("bgpatom")["full_feed_threshold_v6"]


class BGPAtomPeer:
    """Keep track of BGP atoms from a single peer IP.

    For our purposes, a BGP atom is a group of prefixes that share a common AS path
    (excluding the origin ASN). The process of building atoms is straightforward.

    For a prefix x originated by AS C, reachable via AS path [A B C] where A is the ASN
    of the current peer:

        1. Remove any instances of AS-path prepending
        2. Split the origin ASN (C) from the path
        3. Add prefix x to the group of atom [A B]

    RIB table dump and announcement messages are processed like this. For withdrawal
    messages remove the withdrawn prefix from its corresponding atom group.
    Each prefix can only be present in one group at any point in time, otherwise the
    peer would have shared two best paths with the collector, which should never happen.
    """
    def __init__(self, peer_address):
        self.peer_address = peer_address
        # IPv4 or IPv6
        self.address_family = 4
        if ":" in peer_address:
            self.address_family = 6
        self.peer_asn = None
        self.prefixes_count = 0

        self.path_id_to_aspath = dict()
        self.aspath_to_path_id = dict()

        self.prefix_to_path_id = dict()
        self.prefix_to_origin_asn = dict()

    def set_peer_asn(self, peer_asn: str):
        if self.peer_asn is None:
            self.peer_asn = peer_asn
            return
        if self.peer_asn != peer_asn:
            logging.warning(f"peer_asn miss match, previous: {self.peer_asn}, current {peer_asn}")

    def get_path_id(self, atom_aspath: tuple):
        """Get (and create if not exists) a path ID for the specified AS path."""
        if atom_aspath not in self.aspath_to_path_id:
            return self.set_new_aspath(atom_aspath)
        return self.aspath_to_path_id[atom_aspath]

    def set_new_aspath(self, atom_aspath: tuple):
        """Create a new path ID and assign it to the specified AS path.

        The path ID is a simple increasing integer.
        """
        path_id = len(self.aspath_to_path_id)
        self.aspath_to_path_id[atom_aspath] = path_id
        self.path_id_to_aspath[path_id] = atom_aspath
        return path_id

    def get_aspath_by_path_id(self, path_id: int):
        return self.path_id_to_aspath[path_id]

    def update_prefix_status(self, element: dict):
        """Process the specified BGP message and update the prefix status.

        RIB dump and announcement messages add a prefix to the corresponding atom group,
        withdraw messages mark it as deleted.
        """
        prefix = element["fields"]["prefix"]
        message_type = element["type"]

        # Ignore updates for default routes, which should not be shared with the
        # collector in the first place.
        if prefix == "0.0.0.0/0" or prefix == "::/0":
            return

        if message_type == "A" or message_type == "R":
            # Announcements and RIB dump messages add a prefix.
            aspath = element["fields"]["as-path"]
            # AS path is separated with spaces.
            self.update_announcement_message(prefix, aspath.split(" "))
        elif message_type == "W":
            # Withdrawals remove a prefix.
            self.update_withdrawal_message(prefix)

    def update_announcement_message(self, prefix: str, announced_aspath: list):
        """Process a RIB dump or announcement message by adding the prefix to the
        corresponding atom group.

        Remove AS-path prepending, split the origin ASN, and get (or create) the
        corresponding BGP atom.

        Args:
            prefix (str): Announced prefix
            announced_aspath (list): AS path as list of strings.
        """
        deduplicated_aspath = utils.deduplicate_as_path(announced_aspath)
        origin_asn = deduplicated_aspath[-1]
        # The atom path is simply the normal path without the origin ASN.
        atom_encoded_path = tuple(deduplicated_aspath[:-1])

        # Get the atom group and add the prefix.
        path_id = self.get_path_id(atom_encoded_path)
        self.prefix_to_path_id[prefix] = path_id
        self.prefix_to_origin_asn[prefix] = origin_asn

    def update_withdrawal_message(self, prefix):
        self.prefix_to_path_id[prefix] = WITHDRAWN_PATH_ID

    def is_full_feed(self) -> bool:
        """A 'full feed' peer is a peer with a routing table to (roughly) the entire
        Internet. We infer this status by applying rough thresholds.
        """
        if self.address_family == 4:
            return self.prefixes_count > FULL_FEED_PREFIXES_THRESHOLD_v4
        else:
            return self.prefixes_count > FULL_FEED_PREFIXES_THRESHOLD_v6

    def dump_bgpatom(self, timestamp: int):
        """Dump the current state of BGP atom groups as Kafka messages.

        Args:
            timestamp (int): Timestamp to include in Kafka messages.

        Yields:
            dict: Kafka message.
        """
        # Create the actual atom groups.
        bgpatom = self.construct_bgpatom()

        # Dump atom groups by batching prefixes. If a single atom group is larger than
        # PREFIX_IN_ATOM_BATCH_SIZE, the atom is split into multiple messages.
        # This is no problem, since the peer_ip is used as the Kafka key, i.e., all
        # messages for a peer will be on the same partition and the BGPAtomLoader
        # reassembles them.
        for path_id in bgpatom:
            prefixes_batch = list()

            for prefix in bgpatom[path_id]:
                prefixes_batch.append(prefix)
                if len(prefixes_batch) > PREFIXES_IN_ATOM_BATCH_SIZE:
                    yield self.format_dump_data(prefixes_batch, path_id, timestamp)
                    prefixes_batch = list()

            # Dump half-full batch if it exists.
            if prefixes_batch:
                yield self.format_dump_data(prefixes_batch, path_id, timestamp)

    def construct_bgpatom(self):
        """Construct atom groups from the current state.

        Group active prefixes by path and return only data if this peer is full feed,
        i.e., if it can reach enough prefixes.

        Returns:
            dict: Map of path_id to list of (prefix, origin_asn) tuples.
        """
        bgpatom = defaultdict(list)
        self.prefixes_count = 0

        for prefix in self.prefix_to_path_id:
            path_id = self.prefix_to_path_id[prefix]
            if path_id == WITHDRAWN_PATH_ID:
                continue
            self.prefixes_count += 1
            origin_asn = self.prefix_to_origin_asn[prefix]
            bgpatom[path_id].append((prefix, origin_asn))

        if not self.is_full_feed():
            return dict()
        return bgpatom

    def format_dump_data(self, prefixes_batch: list, path_id: int, timestamp: int):
        aspath = self.path_id_to_aspath[path_id]
        return {
            "prefixes": prefixes_batch,
            "aspath": aspath,
            "peer_address": self.peer_address,
            "peer_asn": self.peer_asn,
            "timestamp": timestamp
        }
