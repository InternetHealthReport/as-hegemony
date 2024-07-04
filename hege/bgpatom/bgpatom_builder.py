from hege.bgpatom.bgp_data import consume_ribs_and_update_message_upto
from hege.bgpatom.bgpatom_peer import BGPAtomPeer
from hege.utils import utils
from hege.utils.config import Config

DUMP_INTERVAL = Config.get("bgpatom")["dump_interval"]
BGPATOM_DATA_TOPIC = Config.get("bgpatom")["data_topic"]
BGPATOM_META_DATA_TOPIC = Config.get("bgpatom")["meta_data_topic"]


class BGPAtomBuilder:
    """Read BGP data and keep track of BGP atoms.

    This class reads the RIB and updates topics of the specified collector from Kafka
    for the specified timeframe. It first creates an initial set of BGP atoms from the
    RIB, which is dumped to Kafka. Then it updates the state, based on announce and
    withdraw update messages, and dumps it every DUMP_INTERVAL seconds (based on message
    time, not real time).

    For a detailed description of BGP atoms, check the BGPAtomPeer class.
    """

    def __init__(self, collector, start_timestamp: int, end_timestamp: int):
        self.collector = collector
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        # Map a peer IP to its corresponding BGPAtomPeer object.
        self.bgpatom_peers = dict()

        self.kafka_data_topic = f"{BGPATOM_DATA_TOPIC}_{collector}"
        self.kafka_meta_data_topic = f"{BGPATOM_META_DATA_TOPIC}_{collector}"

    def get_bgpatom_peer(self, peer_address: str) -> BGPAtomPeer:
        """Return (and create if not exist) the BGPAtomPeer object for this peer IP."""
        if peer_address not in self.bgpatom_peers:
            self.set_bgpatom_peer(peer_address)
        return self.bgpatom_peers[peer_address]

    def set_bgpatom_peer(self, peer_address: str) -> None:
        """Create a BGPAtomPeer object for this peer IP and store it in
        bgpatom_peers."""
        self.bgpatom_peers[peer_address] = BGPAtomPeer(peer_address)

    def consume_and_calculate(self):
        """Read BGP RIB and update topics, create BGP atoms, and yield atom state every
        DUMP_INTERVAL seconds.

        Messages are consumed from start_timestamp to end_timestamp - 1 (see bgp_data.py
        for more details). At least one update message must exist, else no dump is
        triggered.

        Yields:
            Tuple[int, Generator]: The timestamp of the current dump and a Generator for
            the BGP atom Kafka messages. See BGPAtomPeer class for the message format.
        """
        next_dumped_timestamp = self.start_timestamp
        for element in consume_ribs_and_update_message_upto(
                self.collector, self.start_timestamp, self.end_timestamp):

            # This is only triggered by messages from the updates topic, since all RIB
            # messages have their 'time' set to start_timestamp.
            if element["time"] > next_dumped_timestamp:
                bgpatom_messages_generator = self.dump_bgpatom_messages(next_dumped_timestamp)
                yield next_dumped_timestamp, bgpatom_messages_generator
                next_dumped_timestamp += DUMP_INTERVAL

            peer_address = element["peer_address"]
            peer_asn = element["peer_asn"]
            # Get or create BGPAtomPeer object for this peer IP.
            bgpatom_peer = self.get_bgpatom_peer(peer_address)
            # Set the peer ASN every time, even if we retrieved an existing object. This
            # will trigger a warning if there is some mismatch.
            bgpatom_peer.set_peer_asn(peer_asn)
            # Update the atom state.
            bgpatom_peer.update_prefix_status(element)

    def dump_bgpatom_messages(self, timestamp: int):
        """Yield formatted BGP atom Kafka messages.

        Messages are returned per peer IP (all atoms of a peer IP are dumped
        sequentially), which is also used as key for the Kafka topic.

        Args:
            timestamp (int): Dump timestamp which is included in the returned messages.

        Yields:
            Tuple[dict, str]: The formatted message and peer IP which is used as key for
            the Kafka topic.
        """
        for peer_address in self.bgpatom_peers:
            bgpatom_peer = self.bgpatom_peers[peer_address]

            for bgpatom_kafka_message in bgpatom_peer.dump_bgpatom(timestamp):
                yield bgpatom_kafka_message, peer_address


if __name__ == "__main__":
    test_collector = "rrc10"

    start_time_string = "2020-08-01T00:00:00"
    start_ts = utils.str_datetime_to_timestamp(start_time_string)

    end_time_string = "2020-08-01T00:16:00"
    end_ts = utils.str_datetime_to_timestamp(end_time_string)

    bgpatom_builder = BGPAtomBuilder(test_collector, start_ts, end_ts)
    for ts, bgpatom_generator in bgpatom_builder.consume_and_calculate():
        for message in bgpatom_generator:
            print(ts, message)
            break
        break
