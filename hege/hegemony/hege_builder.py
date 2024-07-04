from hege.hegemony.hege_builder_helper import HegeBuilderHelper
from hege.utils import utils
from hege.utils.config import Config

AS_HEGE_DATA_TOPIC = Config.get("hege")["data_topic__as"]
AS_HEGE_META_DATA_TOPIC = Config.get("hege")["meta_data_topic__as"]
PREFIX_HEGE_DATA_TOPIC = Config.get("hege")["data_topic__prefix"]
PREFIX_HEGE_META_DATA_TOPIC = Config.get("hege")["meta_data_topic__prefix"]
INTERVAL = Config.get("hege")["dump_interval"]


class HegeBuilder:
    """Calculate Hegemony scores.

    This class reads the BC score topics of the specified collectors from Kafka for the
    specified timeframe. Scores are processed in DUMP_INTERVAL steps and produced per
    scope. For more details see the HegeBuilderHelper class which performs the actual
    calculation.

    This process can be called on a single partition ID to enable parallelization over
    multiple machines. Since the scope is used as the key, all BC scores for one scope
    will be in the same partition independent of the collector topic.
    """

    def __init__(self, collectors, start_timestamp: int, end_timestamp: int,
                 prefix_mode=False, partition_id=None, sparse_peers=False):
        self.collectors = collectors
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.prefix_mode = prefix_mode
        self.partition_id = partition_id
        self.sparse_peers = sparse_peers

        if prefix_mode:
            self.kafka_data_topic = PREFIX_HEGE_DATA_TOPIC
            self.kafka_meta_data_topic = PREFIX_HEGE_META_DATA_TOPIC
        else:
            self.kafka_data_topic = AS_HEGE_DATA_TOPIC
            self.kafka_meta_data_topic = AS_HEGE_META_DATA_TOPIC

    def consume_and_calculate(self):
        """Read the BC score topics for the specified collectors and produce AS Hegemony
        messages in INTERVAL steps.

        Yields:
            Tuple[int, Generator]: The timestamp of the current dump and a Generator for
            the AS Hegemony Kafka messages.
        """
        for timestamp in range(self.start_timestamp, self.end_timestamp, INTERVAL):
            # The helper class does the actual reading of Kafka topics and AS Hegemony
            # calculation for a single timestamp.
            hege_builder_helper = HegeBuilderHelper(self.collectors,
                                                    timestamp,
                                                    self.prefix_mode,
                                                    self.partition_id,
                                                    self.sparse_peers)
            hege_builder_helper.build_hegemony_score()
            yield timestamp, self.dump_as_hegemony_score(hege_builder_helper, timestamp)

    def dump_as_hegemony_score(self, hege_builder_helper, timestamp: int):
        """Return formatted Kafka messages for each scope and each AS in the scope.

        This function should be called after calling calculate_hegemony() on the
        specified HegeBuilderHelper object. Otherwise there are no Hegemony scores
        present and this function returns nothing.

        Args:
            hege_builder_helper (HegeBuilderHelper): The HegeBuilderHelper object which
            calculated the Hegemony scores.
            timestamp (int): Timestamp of the current dump, which is included in the
            Kafka messages.

        Yields:
            Tuple[dict, str]: Formatted Kafka message and scope. Message contains the
            Hegemony score for a single AS towards a single scope.
        """
        hegemony_scope = hege_builder_helper.hegemony_score
        for scope in hegemony_scope:
            for asn, hege in hegemony_scope[scope].items():
                yield self.format_message(hege['hege'], asn, scope, timestamp, hege['nb_peers']), scope

    @staticmethod
    def format_message(hege: float, asn: str, scope: str, timestamp: int, nb_peers: int):
        return {
            "timestamp": timestamp,
            "hege": hege,
            "asn": asn,
            "scope": scope,
            "nb_peers": nb_peers
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
