import utils
from kafkadata import create_consumer_and_set_offset, consume_stream


def consume_ribs_message_at(collector: str, rib_timestamp: int):
    bgp_data_topic = f"ihr_bgp_{collector}_ribs"
    consumer = create_consumer_and_set_offset(bgp_data_topic, rib_timestamp)

    for bgp_msg, _ in consume_stream(consumer):
        dump_timestamp = bgp_msg["rec"]["time"]

        if dump_timestamp != rib_timestamp:
            return

        for element in bgp_msg["elements"]:
            element_type = element["type"]
            assert element_type == "R", "consumer yield none RIBS message"
            yield element


def consume_updates_message_upto(collector: str, start_timestamp: int, end_timestamp: int):
    bgp_data_topic = f"ihr_bgp_{collector}_updates"
    consumer = create_consumer_and_set_offset(bgp_data_topic, start_timestamp)

    # data published at end_timestamp will not be consumed
    for bgp_msg, _ in consume_stream(consumer):
        dump_timestamp = bgp_msg["rec"]["time"]

        if dump_timestamp >= end_timestamp:
            return

        for element in bgp_msg["elements"]:
            element_type = element["type"]
            if element_type == "A" or element_type == "W":
                yield element


def consume_bgp_data_stream(collector: str, start_timestamp: int, end_timestamp: int):
    for element in consume_ribs_message_at(collector, start_timestamp):
        yield element

    for element in consume_updates_message_upto(collector, start_timestamp, end_timestamp):
        yield element


if __name__ == "__main__":
    offset_time_string = "2020-08-01T00:00:00"
    offset_datetime = utils.str2dt(offset_time_string, utils.DATETIME_STRING_FORMAT)
    offset_timestamp = utils.dt2ts(offset_datetime)

    print("consuming ribs message")
    for msg in consume_ribs_message_at("rrc10", offset_timestamp):
        print(msg)
        break

    print("consuming updates message")
    for msg in consume_updates_message_upto("rrc10", offset_timestamp, offset_timestamp + 20):
        print(msg)
