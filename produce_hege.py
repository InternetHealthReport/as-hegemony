import argparse
import logging
import os

from hege.utils.config import Config

if __name__ == "__main__":
    text = """This script consumes the BC scores of the specified collectors and
    produces AS hegemony score between --start_time and --end_time."""

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument("--start_time", "-s", help="Choose the start time")
    parser.add_argument("--end_time", "-e", help="Choose the end time")
    parser.add_argument("--collectors", "-c",
                        help="Choose your collectors: it should be written in the following patterns"
                             "collector1,collector2,collector3")
    parser.add_argument("--prefix", "-p",
                        help="With this flag, the script will run in prefix hege mode",
                        action='store_true')
    parser.add_argument("--partition_id", help="Select only one kafka partition")
    parser.add_argument("--sparse_peers", help="Do not assume full-feed peers",
                        action='store_true')
    parser.add_argument("--config_file", "-C",
                        help="Path to the configuration file")

    args = parser.parse_args()
    assert args.start_time and args.end_time

    selected_collectors = list(map(lambda x: x.strip(), args.collectors.split(",")))
    start_time_string = args.start_time
    end_time_string = args.end_time
    Config.load(args.config_file)

    if args.prefix:
        log_filename_suffix = "prefix"
    else:
        log_filename_suffix = "asn"

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logDir = '/log/'
    if not os.path.exists(logDir):
        logDir = './'
    logging.basicConfig(
        format=FORMAT, filename=f"{logDir}/ihr-kafka-hegemony-{start_time_string}-{log_filename_suffix}.log",
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
    )

    # import after the config parameters are fully loaded
    from hege.hegemony.hege_builder import HegeBuilder
    from hege.utils import utils
    from hege.utils.data_producer import DataProducer

    start_ts = utils.str_datetime_to_timestamp(start_time_string)
    end_ts = utils.str_datetime_to_timestamp(end_time_string)

    hege_builder = HegeBuilder(selected_collectors, start_ts, end_ts, args.prefix, args.partition_id, args.sparse_peers)
    hege_data_producer = DataProducer(hege_builder)
    hege_data_producer.produce_kafka_messages_between()
