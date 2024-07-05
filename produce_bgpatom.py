import argparse
import logging
import os

from hege.utils.config import Config

if __name__ == "__main__":
    text = """This script consumes BGP Data from the selected collector and produces BGP
    atoms between --start_time and --end_time. It then analyzes and publishes the BGP
    atoms to the Kafka cluster"""

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument("--collector", "-c", help="Choose collector to push data for")
    parser.add_argument("--start_time", "-s", help="Choose the start time")
    parser.add_argument("--end_time", "-e", help="Choose the end time ")
    parser.add_argument("--config_file", "-C",
                        help="Path to the configuration file")

    args = parser.parse_args()
    assert args.start_time and args.collector and args.end_time

    selected_collector = args.collector
    start_time_string = args.start_time
    end_time_string = args.end_time
    Config.load(args.config_file)

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logDir = '/log/'
    if not os.path.exists(logDir):
        logDir = './'
    logging.basicConfig(
        format=FORMAT, filename=f"{logDir}/ihr-kafka-bgpatom_{start_time_string}-{selected_collector}.log",
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
    )

    # import after the config parameters are fully loaded
    from hege.bgpatom.bgpatom_builder import BGPAtomBuilder
    from hege.utils import utils
    from hege.utils.data_producer import DataProducer

    start_ts = utils.str_datetime_to_timestamp(start_time_string)
    end_ts = utils.str_datetime_to_timestamp(end_time_string)

    bgpatom_builder = BGPAtomBuilder(selected_collector, start_ts, end_ts)
    bgpatom_data_producer = DataProducer(bgpatom_builder)
    bgpatom_data_producer.produce_kafka_messages_between()
