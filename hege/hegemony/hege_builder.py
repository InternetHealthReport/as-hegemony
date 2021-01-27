import json

from hege.hegemony.hege_builder_helper import HegeBuilderHelper
from hege.utils import utils

with open("/app/config.json", "r") as f:
    config = json.load(f)
AS_HEGE_DATA_TOPIC = config["hege"]["data_topic__as"]
AS_HEGE_META_DATA_TOPIC = config["hege"]["meta_data_topic__as"]
PREFIX_HEGE_DATA_TOPIC = config["hege"]["data_topic__prefix"]
PREFIX_HEGE_META_DATA_TOPIC = config["hege"]["meta_data_topic__prefix"]
INTERVAL = config["hege"]["dump_interval"]


class HegeBuilder:
    def __init__(self, collectors, start_timestamp: int, end_timestamp: int, prefix_mode=False):
        self.collectors = collectors
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.prefix_mode = prefix_mode

        if prefix_mode:
            self.kafka_data_topic = PREFIX_HEGE_DATA_TOPIC
            self.kafka_meta_data_topic = PREFIX_HEGE_META_DATA_TOPIC
        else:
            self.kafka_data_topic = AS_HEGE_DATA_TOPIC
            self.kafka_meta_data_topic = AS_HEGE_META_DATA_TOPIC

    def consume_and_calculate(self):
        for timestamp in range(self.start_timestamp, self.end_timestamp, INTERVAL):
            hege_builder_helper = HegeBuilderHelper(self.collectors, timestamp, self.prefix_mode)
            hege_builder_helper.build_hegemony_score()
            yield timestamp, self.dump_as_hegemony_score(hege_builder_helper, timestamp)

    def dump_as_hegemony_score(self, hege_builder_helper, timestamp: int):
        hegemony_scope = hege_builder_helper.hegemony_score
        for scope in hegemony_scope:
            yield self.format_message(hegemony_scope[scope], scope, timestamp), scope

    @staticmethod
    def format_message(scope_hege: dict, scope: str, timestamp: int):
        return {
            "scope_hegemony": scope_hege,
            "timestamp": timestamp,
            "scope": scope
        }


if __name__ == "__main__":
    start_time_string = "2020-08-01T00:00:00"
    start_ts = utils.str_datetime_to_timestamp(start_time_string)

    # test_collectors = ["rrc00", "rrc10", "route-views.linx", "route-views2"]
    test_collectors = ["rrc10"]
    hege_builder = HegeBuilder(test_collectors, start_ts, start_ts + 60, True)

    for ts, hege_generator in hege_builder.consume_and_calculate():
        print(ts)
        for message, scope_asn in hege_generator:
            print(scope_asn, message)
            break
        break
