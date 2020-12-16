import logging
import json
import argparse

from hege.utils import kafka_data, utils

with open("/app/config.json", "r") as f:
    config = json.load(f)
ASHEGE_TOPIC = config["hege"]["data_topic"]


def __get_hege_consumer(start_timestamp: int):
    return kafka_data.create_consumer_and_set_offset(ASHEGE_TOPIC, start_timestamp)


def get_hegemony_score_for_asn_at(asn: str, timestamp: int):
    consumer = __get_hege_consumer(timestamp)
    for message, _ in kafka_data.consume_stream(consumer):
        message_timestamp = message["timestamp"]
        scope_asn = message["scope"]
        if message_timestamp != timestamp:
            return
        if asn == scope_asn:
            return message["scope_hegemony"]


def get_hegemony_score_at(timestamp: int):
    hege_score = dict()
    consumer = __get_hege_consumer(timestamp)
    for message, _ in kafka_data.consume_stream(consumer):
        message_timestamp = message["timestamp"]
        scope_asn = message["scope"]
        if message_timestamp != timestamp:
            break
        hege_score[scope_asn] = message["scope_hegemony"]
    return hege_score


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--selected_time", "-t", help="choose the as hegemony time")
    parser.add_argument("--asn", "-a", help="choose asn")
    # Example: 2020-08-01T00:00:00

    args = parser.parse_args()
    assert args.selected_time

    selected_timestamp = utils.str_datetime_to_timestamp(args.selected_time)
    scope_asn = args.asn

    if scope_asn:
        print(get_hegemony_score_for_asn_at(scope_asn, selected_timestamp))
    else:
        print(get_hegemony_score_at(selected_timestamp))
