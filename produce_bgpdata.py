import sys
import os
import argparse
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pybgpstream import BGPStream
from datetime import datetime
from datetime import timedelta
import msgpack
import logging
import json

from hege.utils.config import Config

# Initialized in the __main__
RIB_BUFFER_INTERVAL = None 
BGP_DATA_TOPIC_PREFIX = None
DATA_RETENTION = None


def dt2ts(dt):
    return int((dt - datetime(1970, 1, 1)).total_seconds())


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        pass


def get_record_dict(record):
    record_dict = dict()

    record_dict["project"] = record.project
    record_dict["collector"] = record.collector
    record_dict["type"] = record.type
    record_dict["dump_time"] = record.dump_time
    record_dict["time"] = record.time
    record_dict["status"] = record.status
    record_dict["dump_position"] = record.dump_position

    return record_dict


def get_element_dict(element):
    element_dict = dict()

    element_dict["type"] = element.type
    element_dict["time"] = element.time
    element_dict["peer_asn"] = element.peer_asn
    element_dict["peer_address"] = element.peer_address
    element_dict["fields"] = element.fields
    if 'communities' in element.fields:
        element_dict['fields']['communities'] = list(element.fields['communities'])

    return element_dict


def push_data(record_type, collector, startts, endts):
    stream = BGPStream(
        from_time=str(startts), until_time=str(endts), collectors=[collector],
        record_type=record_type
    )

    # Create kafka topic
    topic = BGP_DATA_TOPIC_PREFIX + "_" + collector + "_" + record_type
    admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})

    topic_list = [NewTopic(topic, num_partitions=1, replication_factor=1, config={"retention.ms": DATA_RETENTION})]
    created_topic = admin_client.create_topics(topic_list)

    for topic, f in created_topic.items():
        try:
            f.result()  # The result itself is None
            logging.warning("Topic {} created".format(topic))
        except Exception as e:
            logging.warning("Failed to create topic {}: {}".format(topic, e))

    # Create producer
    producer = Producer({'bootstrap.servers': 'localhost:9092',
                         # 'linger.ms': 1000,
                         'default.topic.config': {'compression.codec': 'snappy'}})

    for rec in stream.records():
        completeRecord = {}
        completeRecord["rec"] = get_record_dict(rec)
        completeRecord["elements"] = []

        recordTimeStamp = int(rec.time * 1000)

        for elem in rec:
            elementDict = get_element_dict(elem)
            completeRecord["elements"].append(elementDict)

        try:
            producer.produce(
                topic,
                msgpack.packb(completeRecord, use_bin_type=True),
                callback=delivery_report,
                timestamp=recordTimeStamp
                )

            # Trigger any available delivery report callbacks from previous produce() calls
            producer.poll(0)

        except BufferError:
            logging.warning('buffer error, the queue must be full! Flushing...')
            producer.flush()

            logging.info('queue flushed, try re-write previous message')

            producer.produce(
                topic,
                msgpack.packb(completeRecord, use_bin_type=True),
                callback=delivery_report,
                timestamp=recordTimeStamp
            )
            producer.poll(0)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    producer.flush()


if __name__ == '__main__':

    text = "This script pushes BGP data from specified collector(s) \
for the specified time window to Kafka topic(s). The created topics have only \
one partition in order to make sequential reads easy. If no start and end time \
is given then it download data for the current hour."

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument("--collector", "-c", help="Choose collector to push data for")
    parser.add_argument("--startTime", "-s",
                        help="Choose start time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")
    parser.add_argument("--endTime", "-e", help="Choose end time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")
    parser.add_argument("--type", "-t", help="Choose record type: ribs or updates")
    parser.add_argument("--config_file", "-C",
                        help="Path to the configuration file")

    args = parser.parse_args()
    Config.load(args.config_file)

    RIB_BUFFER_INTERVAL = Config.get("bgp_data")["rib_buffer_interval"]
    BGP_DATA_TOPIC_PREFIX = Config.get("bgp_data")["data_topic"]
    DATA_RETENTION = Config.get("kafka")["default_topic_config"]["config"]["retention.ms"]

    # initialize recordType
    recordType = ""
    if args.type:
        if args.type in ["ribs", "updates"]:
            recordType = args.type
        else:
            sys.exit("Incorrect type specified; Choose from rib or update")
    else:
        sys.exit("Record type not specified")

    # initialize collector
    if args.collector:
        collector = args.collector
    else:
        sys.exit("Collector(s) not specified")

    # initialize time to start
    timeWindow = 15
    currentTime = datetime.utcnow()
    minuteStart = int(currentTime.minute / timeWindow) * timeWindow
    timeStart = ""
    if args.startTime:
        timeStart = args.startTime
    else:
        if recordType == 'updates':
            timeStart = currentTime.replace(microsecond=0, second=0, minute=minuteStart) - timedelta(
                minutes=2 * timeWindow)
        else:
            delay = 120
            if 'rrc' in collector:
                delay = 480
            timeStart = currentTime - timedelta(minutes=delay)

    # initialize time to end
    timeEnd = ""
    if args.endTime:
        timeEnd = args.endTime
    else:
        if recordType == 'updates':
            timeEnd = currentTime.replace(microsecond=0, second=0, minute=minuteStart) - timedelta(
                minutes=1 * timeWindow)
        else:
            timeEnd = currentTime

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logDir = '/log/'
    if not os.path.exists(logDir):
        logDir = './'
    logging.basicConfig(
        format=FORMAT, filename=f'{logDir}/ihr-kafka-bgpstream2_{collector}.log',
        level=logging.WARN, datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.warning("Started: %s" % sys.argv)
    logging.warning("Arguments: %s" % args)
    logging.warning('start time: {}, end time: {}'.format(timeStart, timeEnd))

    logging.warning("Downloading {} data for {}".format(recordType, collector))
    push_data(recordType, collector, timeStart, timeEnd)

    logging.warning("End: %s" % sys.argv)

    # python3 /app/produce_bgpdata.py -t ribs --collector rrc10 --startTime 2020-08-01T00:00:00 --endTime 2020-08-02T00:00:00
    # python3 /app/produce_bgpdata.py -t updates --collector rrc10 --startTime 2020-08-01T00:00:00 --endTime 2020-08-02T00:00:00
