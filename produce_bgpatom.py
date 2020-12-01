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
PREFIXES_IN_ATOM_BATCH_SIZE = config["bgpatom"]["prefixes_in_atom_batch_size"]
BGPATOM_METADATA_TOPIC = config["bgpatom"]["metadata_topic"]


def produce_bgpatom_and_metadata(collector: str, timestamp: int):
    bgpatom = construct_bgpatom(collector, timestamp)

    produce_bgpatom(collector, timestamp, bgpatom)
    produce_bgpatom_metadata(collector, timestamp, bgpatom)


def construct_bgpatom(collector, timestamp: int):
    logging.debug("start constructing bgpatom")
    bgpatom_builder = BGPAtomBuilder(collector, timestamp)
    bgpatom_builder.read_ribs_and_add_particles_to_atom()
    bgpatom_builder.remove_none_full_fleet_particles(BGPATOM_FULL_FLEET_THRESHOLD)
    bgpatom = bgpatom_builder.dump_bgpatom()
    return bgpatom


def produce_bgpatom(collector: str, timestamp: int, bgpatom: dict):
    producer = prepare_producer()

    bgpatom_topic = f"ihr_bgp_atom_{collector}"
    create_topic(bgpatom_topic)
    ms_timestamp = timestamp * 1000

    logging.debug(f"start publishing bgpatom to {bgpatom_topic}")
    for dump_batch in get_bgpatom_prefixes_batch(bgpatom):
        producer.produce(
            bgpatom_topic,
            msgpack.packb(dump_batch, use_bin_type=True),
            callback=__delivery_report,
            timestamp=ms_timestamp
        )


def produce_bgpatom_metadata(collector: str, timestamp: int, bgpatom: dict):
    producer = prepare_producer()

    create_topic(BGPATOM_METADATA_TOPIC)
    ms_timestamp = timestamp * 1000

    bgpatom_meta = {
        "total_number_of_atom": len(bgpatom),
        "collector": collector,
        "timestamp": timestamp
    }

    logging.debug(f"start publishing bgpatom meta to {BGPATOM_METADATA_TOPIC}")
    producer.produce(
        BGPATOM_METADATA_TOPIC,
        msgpack.packb(bgpatom_meta, use_bin_type=True),
        key=collector,
        callback=__delivery_report,
        timestamp=ms_timestamp
    )


def get_bgpatom_prefixes_batch(bgpatom: dict):
    for atom_id, atom in enumerate(bgpatom):
        prefixes_batch = list()
        for prefix in bgpatom[atom]:
            prefixes_batch.append(prefix)
            if len(prefixes_batch) > PREFIXES_IN_ATOM_BATCH_SIZE:
                yield format_dump_data(prefixes_batch, atom_id, atom)
                prefixes_batch = list()

        if prefixes_batch:
            yield format_dump_data(prefixes_batch, atom_id, atom)


def format_dump_data(prefixes: list, atom_id: int, atom: str):
    return {
        "prefixes": prefixes,
        "atom_id": atom_id,
        "atom": atom
    }


def __delivery_report(err, _):
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        pass


if __name__ == "__main__":
    text = """This script consumes BGP RIB from inputted collector(s) 
    at the specified time. It then analyzes and publishes BGP atom to 
    kafka"""

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument("--collector", "-c", help="Choose collector to push data for")
    parser.add_argument("--ribs_time", "-t",
                        help="Choose the ribs dumped time   (Format: Y-m-dTH:M:S; Example: 2020-08-01T00:00:00)")

    args = parser.parse_args()
    assert args.ribs_time and args.collector, "collector (-c) and ribs_time (-t) must be entered"
    selected_collector = args.collector
    ribs_time_string = args.ribs_time

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT, filename=f"/log/ihr-kafka-bgpatom_{selected_collector}.log",
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
    )

    bgpatom_time_string = ribs_time_string
    bgpatom_datetime = utils.str2dt(bgpatom_time_string, utils.DATETIME_STRING_FORMAT)
    bgpatom_timestamp = utils.dt2ts(bgpatom_datetime)

    produce_bgpatom_and_metadata(selected_collector, bgpatom_timestamp)
