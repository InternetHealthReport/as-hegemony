from hege.utils import utils
from hege.utils.config import Config
from hege.utils.kafka_data import consume_stream, create_consumer_and_set_offset

RIB_BUFFER_INTERVAL = Config.get("bgp_data")["rib_buffer_interval"]
BGP_DATA_TOPIC_PREFIX = Config.get("bgp_data")["data_topic"]


def consume_ribs_message_at(collector: str, rib_timestamp: int):
    """Read messages from the RIB topic of the collector, starting at rib_timestamp.

    Messages in the interval [rib_timestamp, rib_timestamp + RIB_BUFFER_INTERVAL] will
    be read, but all returned elements have the time set to rib_timestamp. This
    allows us to use the same config file for multiple collectors that do not produce
    RIBs at precisely the same time.

    Args:
        collector (str): Name of the collector
        rib_timestamp (int): Start timestamp of read window and timestamp that is forced
        in returned elements.

    Yields:
        dict: Each returned element represents a BGP RIB table dump message with the
        following format (example):

        {
            'type': 'R',
            'time': 1717113600.0,
            'peer_asn': 34549,
            'peer_address': '80.77.16.114',
            'fields': {
            'next-hop': '80.77.16.114',
            'as-path': '34549 174 14068',
            'communities': ['174:22013', '34549:100', '174:21001', '34549:174'],
            'prefix': '216.163.136.0/24'
        }
    """
    bgp_data_topic = f"{BGP_DATA_TOPIC_PREFIX}_{collector}_ribs"
    consumer = create_consumer_and_set_offset(bgp_data_topic, rib_timestamp)

    for bgp_msg, _ in consume_stream(consumer, rib_timestamp+RIB_BUFFER_INTERVAL):
        # dump_timestamp = bgp_msg["rec"]["time"]

        #        if dump_timestamp - rib_timestamp > RIB_BUFFER_INTERVAL:
        #            return dict()

        for element in bgp_msg["elements"]:
            element_type = element["type"]
            element["time"] = rib_timestamp
            assert element_type == "R", "consumer yield none RIBS message"
            yield element

    return dict()


def consume_updates_message_upto(collector: str, start_timestamp: int, end_timestamp: int):
    """Read messages from the update topic of the collector in the interval
    [start_timestamp, end_timestamp - 1].

    Args:
        collector (str): Name of the collector
        start_timestamp (int): Start timestamp for read
        end_timestamp (int): End timestamp for read (exclusive).

    Yields:
        dict: Each returned element represents a BGP update message, which is either an
        announcement or a withdrawal. Announcements have the same format as RIB table
        dump messages (only with type: 'A'; see above), withdrawals contain fewer
        fields:

        {
            'type': 'W',
            'time': 1717376174.0,
            'peer_asn': 49673,
            'peer_address': '2a02:47a0:a::1',
            'fields': {
                'prefix': '2c0f:f698:c450::/44'
            }
        }
    """
    bgp_data_topic = f"{BGP_DATA_TOPIC_PREFIX}_{collector}_updates"
    consumer = create_consumer_and_set_offset(bgp_data_topic, start_timestamp)

    # data published at end_timestamp will not be consumed
    for bgp_msg, _ in consume_stream(consumer, end_timestamp-1):
        # dump_timestamp = bgp_msg["rec"]["time"]

        # if dump_timestamp >= end_timestamp:
        # return dict()

        for element in bgp_msg["elements"]:
            element_type = element["type"]
            if element_type == "A" or element_type == "W":
                yield element

    return dict()


def consume_ribs_and_update_message_upto(collector: str, start_timestamp: int, end_timestamp: int):
    """Helper function that reads from both RIB and updates topics.

    RIB topic is read in the interval [start_timestamp, start_timestamp +
    RIB_BUFFER_INTERVAL] and all returned elements have time set to start_timestamp.
    Updates topic is read in [start_timestamp, end_timestamp - 1].

    Args:
        collector (str): Name of the collector
        start_timestamp (int): Start timestamp for read
        end_timestamp (str): End timestamp for read (exclusive)

    Yields:
        dict: Each return element represents either a BGP RIB dump message, or a BGP
        update message. For the specific formats see description of the other functions.
    """
    for element in consume_ribs_message_at(collector, start_timestamp):
        yield element

    for element in consume_updates_message_upto(collector, start_timestamp, end_timestamp):
        yield element


if __name__ == "__main__":
    offset_time_string = "2020-08-01T00:00:00"
    offset_timestamp = utils.str_datetime_to_timestamp(offset_time_string)

    print("consuming ribs message")
    for msg in consume_ribs_message_at("rrc10", offset_timestamp):
        print(msg)
        break

    print("consuming updates message")
    for msg in consume_updates_message_upto("rrc10", offset_timestamp, offset_timestamp + 20):
        print(msg)
