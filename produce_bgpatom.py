import argparse
import logging
import os

from hege.bgpatom.bgpatom_builder import BGPAtomBuilder
from hege.utils.data_producer import DataProducer
from hege.utils import utils

if __name__ == "__main__":
    text = """This script consumes BGP Data from selected collector(s) 
    and produce bgpatom between --start_time and --end_time. It then 
    analyzes and publishes BGP atom to kafka cluster"""

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
    logDir = '/log/'
    if not os.path.exists(logDir):
        logDir = './'
    logging.basicConfig(
        format=FORMAT, filename=f"{logDir}/ihr-kafka-bgpatom_{start_time_string}-{selected_collector}.log",
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
    )

    start_ts = utils.str_datetime_to_timestamp(start_time_string)
    end_ts = utils.str_datetime_to_timestamp(end_time_string)

    bgpatom_builder = BGPAtomBuilder(selected_collector, start_ts, end_ts)
    bgpatom_data_producer = DataProducer(bgpatom_builder)
    bgpatom_data_producer.produce_kafka_messages_between()
