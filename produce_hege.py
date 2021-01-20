import argparse
import logging

from hege.hegemony.hege_builder import HegeBuilder
from hege.utils.data_producer import DataProducer
from hege.utils import utils

if __name__ == "__main__":
    text = """This script consumes all collectors bcscore and produce
    as hegemony score between --start_time and --end_time."""

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument("--start_time", "-s", help="Choose the start time")
    parser.add_argument("--end_time", "-e", help="Choose the end time")
    parser.add_argument("--collectors", "-c",
                        help="Choose your collectors: it should be written in the following patterns"
                             "collector1,collector2,collector3")
    parser.add_argument("--prefix", "-p",
                        help="if you used this flag, script will run in prefix hege mode",
                        action='store_true')

    # Example: 2020-08-01T00:00:00

    args = parser.parse_args()
    assert args.start_time and args.end_time

    selected_collectors = list(map(lambda x: x.strip(), args.collectors.split(",")))
    start_time_string = args.start_time
    end_time_string = args.end_time

    FORMAT = '%(asctime)s %(processName)s %(message)s'

    log_filename_suffix = "prefix" if args.prefix else "asn"
    logging.basicConfig(
        format=FORMAT, filename=f"/log/ihr-kafka-hegemony-{log_filename_suffix}.log",
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
    )

    start_ts = utils.str_datetime_to_timestamp(start_time_string)
    end_ts = utils.str_datetime_to_timestamp(end_time_string)

    hege_builder = HegeBuilder(selected_collectors, start_ts, end_ts, args.prefix)
    hege_data_producer = DataProducer(hege_builder)
    hege_data_producer.produce_kafka_messages_between()
