from collections import defaultdict
from functools import partial
import argparse
import json
import logging
import msgpack

import utils
from kafkadata import create_topic, prepare_producer
from hege.bgpatom.bgpatom_builder import BGPAtomBuilder

with open("config.json", "r") as f:
    config = json.load(f)
KAFKA_BOOTSTRAP_SERVERS = config["kafka"]["bootstrap_servers"]
BGPATOM_FULL_FLEET_THRESHOLD = config["bgpatom"]["full_fleet_threshold"]
FULL_FLEET_THRESHOLD = config["bgpatom"]["full_fleet_threshold"]
BGPATOM_META_DATA_TOPIC = config["bgpatom"]["meta_data_topic"]

messages_per_peer = defaultdict(int)


def produce_bgpatom_between(collector: str, start_timestamp: int, end_timestamp: int):
    bgpatom_builder = BGPAtomBuilder(collector, start_timestamp, end_timestamp)
    bgpatom_topic = f"ihr_bgp_atom_{collector}"
    create_topic(bgpatom_topic)

    logging.debug(f"start dumping bgpatom to {bgpatom_topic}, between {start_timestamp} and {end_timestamp}")

    for timestamp in bgpatom_builder.read_bgp_message_and_construct_atom():
        producer = prepare_producer()
        bgpatom_message_generator = bgpatom_builder.dump_bgpatom_messages(timestamp)
        produce_bgpatom_at(producer, bgpatom_message_generator, bgpatom_topic, timestamp)

    logging.debug(f"successfully dumped bgpatom: ({bgpatom_topic}, {start_timestamp} - {end_timestamp})")


def produce_bgpatom_at(producer, bgpatom_message_generator, bgpatom_topic: str, timestamp: int):
    logging.debug(f"({bgpatom_topic}, {timestamp}): start producing ...")

    produce_bgpatom_data_at(producer, bgpatom_message_generator, bgpatom_topic, timestamp)
    produce_bgpatom_metadata_at(producer, timestamp)
    producer.flush()

    logging.debug(f"({bgpatom_topic}, {timestamp}): DONE")


def produce_bgpatom_data_at(producer, bgpatom_message_generator, bgpatom_topic:str,  timestamp: int):
    ms_timestamp = timestamp * 1000
    global messages_per_peer
    messages_per_peer = dict()

    for message, peer_address in bgpatom_message_generator:
        delivery_report = partial(__delivery_report, peer_address)
        producer.produce(
            bgpatom_topic,
            msgpack.packb(message, use_bin_type=True),
            peer_address,
            callback=delivery_report,
            timestamp=ms_timestamp
        )
        producer.poll(0)


def produce_bgpatom_metadata_at(producer, timestamp: int):
    ms_timestamp = timestamp * 1000
    global messages_per_peer
    messages_per_peer = dict()

    kafka_message = {
        "messages_per_peer": messages_per_peer,
        "timestamp": timestamp
    }
    producer.produce(
        BGPATOM_META_DATA_TOPIC,
        msgpack.packb(kafka_message, use_bin_type=True),
        callback=__meta_delivery_report,
        timestamp=ms_timestamp
    )
    producer.poll(0)


def __delivery_report(peer_address, err, _):
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        global messages_per_peer
        if peer_address not in messages_per_peer:
            messages_per_peer[peer_address] = 0
        messages_per_peer[peer_address] += 1


def __meta_delivery_report(err, _):
    if err is not None:
        logging.error('metadata delivery failed: {}'.format(err))
    else:
        pass


if __name__ == "__main__":
    text = """This script consumes BGP RIB from inputted collector(s) 
    at the specified time. It then analyzes and publishes BGP atom to 
    kafka"""

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument("--collector", "-c", help="Choose collector to push data for")
    parser.add_argument("--start_time", "-s", help="Choose the start time")
    parser.add_argument("--end_time", "-e", help="Choose the end time ")
    # Example: 2020-08-01T00:00:00

    args = parser.parse_args()
    assert args.start_time and args.collector and args.end_time

    selected_collector = args.collector
    start_time_string = args.start_time
    end_time_string = args.end_time

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT, filename=f"/log/ihr-kafka-bgpatom_{selected_collector}.log",
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
    )

    start_ts = utils.str_datetime_to_timestamp(start_time_string)
    end_ts = utils.str_datetime_to_timestamp(end_time_string)

    produce_bgpatom_between(selected_collector, start_ts, end_ts)
