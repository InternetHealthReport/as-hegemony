import json
import logging
from collections import defaultdict

import kafkadata
import utils


with open("config.json", "r") as f:
    config = json.load(f)
BGPATOM_METADATA_TOPIC = config["bgpatom"]["metadata_topic"]


def load_multiple_bgpatom_and_merge(collectors: list, timestamp: int):
    pass
    # for collector in collectors:
    #     bgpatom[collector] = load_bgpatom(collector, timestamp)


def load_bgpatom(collector: str, timestamp: int):
    collector_bgpatom = defaultdict(list)

    bgpatom_topic = f"ihr_bgp_atom_{collector}"
    consumer = kafkadata.create_consumer_and_set_offset(bgpatom_topic, timestamp)
    for message, _ in kafkadata.consume_stream(consumer):
        message_timestamp = message["timestamp"]
        if message_timestamp > timestamp:
            break

        bgpatom_id = message["atom_id"]
        atom_prefixes = message["prefixes"]
        collector_bgpatom[bgpatom_id] += atom_prefixes

    logging.debug("bgpatom loaded, crosscheck with metadata")
    if confirm_loaded_bgpatom(collector, timestamp, len(collector_bgpatom)):
        return collector_bgpatom
    else:
        logging.error("loaded bgp atom and metadata are not matched")
        return None


def confirm_loaded_bgpatom(collector: str, timestamp: int, atom_count: int):
    consumer = kafkadata.create_consumer_and_set_offset(BGPATOM_METADATA_TOPIC, timestamp)

    for message, _ in kafkadata.consume_stream(consumer):
        if message["timestamp"] > timestamp:
            logging.error(f"bgpatom meta data not found: {collector}-{timestamp}")
            return False

        meta_data_collector_id = message["collector"]
        if meta_data_collector_id == collector:
            return message["total_number_of_atom"] == atom_count


if __name__ == "__main__":
    bgpatom_time_string = "2020-08-01T00:00:00"
    bgpatom_datetime = utils.str2dt(bgpatom_time_string, utils.DATETIME_STRING_FORMAT)
    bgpatom_timestamp = utils.dt2ts(bgpatom_datetime)

    bgpatom = load_bgpatom("rrc10", bgpatom_timestamp)
    print(f"completed: {len(bgpatom)} loaded")
