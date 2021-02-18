import json
from hege.utils import utils
from hege.utils.kafka_data import create_consumer_and_set_offset, consume_stream

try:
    with open("/app/config.json", "r") as f:
        config = json.load(f)
except FileNotFoundError:
    with open("./config.json", "r") as f:
        config = json.load(f)

RIB_BUFFER_INTERVAL = config["bgp_data"]["rib_buffer_interval"]


def consume_ribs_message_at(collector: str, rib_timestamp: int):
    bgp_data_topic = f"ihr_bgp_{collector}_ribs"
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
    bgp_data_topic = f"ihr_bgp_{collector}_updates"
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
