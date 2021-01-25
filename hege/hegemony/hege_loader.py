import logging
import json
import argparse

from hege.utils import kafka_data, utils

with open("/app/config.json", "r") as f:
    config = json.load(f)
AS_HEGE_TOPIC = config["hege"]["data_topic__as"]
PREFIX_HEGE_TOPIC = config["hege"]["data_topic__prefix"]


def __get_hege_consumer(start_timestamp: int, is_asn_query):
    if is_asn_query:
        return kafka_data.create_consumer_and_set_offset(AS_HEGE_TOPIC, start_timestamp)
    else:
        return kafka_data.create_consumer_and_set_offset(PREFIX_HEGE_TOPIC, start_timestamp)


def get_hegemony_score_for_scope_at(selected_scope: str, timestamp: int):
    is_asn_query = "as" in selected_scope or "AS" in selected_scope
    consumer = __get_hege_consumer(timestamp, is_asn_query)

    for message, _ in kafka_data.consume_stream(consumer):
        message_timestamp = message["timestamp"]
        message_scope = message["scope"]
        if message_timestamp != timestamp:
            return
        if message_scope == selected_scope:
            print(f"found hegemony score for {selected_scope}")
            return message["scope_hegemony"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--selected_time", "-t", help="choose the as hegemony time")
    parser.add_argument("--scope", "-s", help="choose scope")
    # Example: 2020-08-01T00:00:00

    args = parser.parse_args()
    assert args.selected_time

    selected_timestamp = utils.str_datetime_to_timestamp(args.selected_time)
    scope = args.scope

    if scope:
        print(f"search for hegemony score for {scope}")
        print(get_hegemony_score_for_scope_at(scope, selected_timestamp))
    else:
        print("please select scope")
