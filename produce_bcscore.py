import argparse
import logging
import os

from hege.utils.config import Config

if __name__ == "__main__":
    text = """This script consumes BGP Data from selected collector(s) 
    and produce bgpatom between --start_time and --end_time. It then 
    analyzes and publishes BGP atom to kafka cluster"""

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument("--collector", "-c", help="Choose collector to push data for")
    parser.add_argument("--start_time", "-s", help="Choose the start time")
    parser.add_argument("--end_time", "-e", help="Choose the end time ")
    parser.add_argument("--ip_version", "-v",
                        help="Address family to analyze IPv4 or IPv6. Value should be 4 or 6.",
                        default=4)
    parser.add_argument("--prefix", "-p",
                        help="With this flag, the script will run in prefix hege mode",
                        action='store_true')
    parser.add_argument("--config_file", "-C",
                        help="Path to the configuration file",)

    args = parser.parse_args()
    assert args.start_time and args.collector and args.end_time

    selected_collector = args.collector
    start_time_string = args.start_time
    end_time_string = args.end_time
    prefix_mode = args.prefix
    address_family = int(args.ip_version)
    Config.load(args.config_file)

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logDir = '/log/'
    if not os.path.exists(logDir):
        logDir = './'
    logging.basicConfig(
        format=FORMAT, filename=f"{logDir}/ihr-kafka-bcscore_{start_time_string}-{selected_collector}.log",
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
    )

    # import after the config parameters are fully loaded
    from hege.bcscore.bcscore_builder import BCScoreBuilder
    from hege.utils.data_producer import DataProducer
    from hege.utils import utils

    start_ts = utils.str_datetime_to_timestamp(start_time_string)
    end_ts = utils.str_datetime_to_timestamp(end_time_string)

    bcscore_builder = BCScoreBuilder(selected_collector, start_ts, end_ts, prefix_mode, address_family)
    bcscore_data_producer = DataProducer(bcscore_builder)
    bcscore_data_producer.produce_kafka_messages_between()
