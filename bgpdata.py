from confluent_kafka import Consumer, TopicPartition

import logging
import msgpack
import json
import utils

with open("config.json", "r") as f:
    config = json.load(f)
KAFKA_BOOTSTRAP_SERVERS = config["kafka"]["bootstrap_servers"]
NO_NEW_MESSAGE_LIMIT = config["kafka"]["no_new_message_limit"]


def consume_rib_message_at(consumer: Consumer, rib_timestamp: int):
    for bgp_msg in consume_stream(consumer):
        dump_timestamp = bgp_msg["rec"]["time"]

        if dump_timestamp != rib_timestamp:
            return

        for element in bgp_msg["elements"]:
            element_type = element["type"]
            assert element_type == "R", "consumer yield none RIBS message"
            yield element


def consume_stream(consumer: Consumer):
    number_of_empty_message = 0
    while True:
        kafka_msg = consumer.poll(1.0)

        if kafka_msg is None:
            number_of_empty_message += 1
            if number_of_empty_message > NO_NEW_MESSAGE_LIMIT:
                return
            continue

        if kafka_msg.error():
            logging.error(f"consumer error {kafka_msg.error()}")
            continue

        bgp_record = msgpack.unpackb(kafka_msg.value(), raw=False)
        yield bgp_record


def set_consumer_time_offset(consumer: Consumer, topic: str, timestamp: int):
    time_offset = consumer.offsets_for_times(
        [TopicPartition(
            topic,
            partition=0,
            offset=timestamp
        )], timeout=1)

    if time_offset == -1:
        raise Exception("cannot assign topic partition")

    consumer.assign(time_offset)


def create_consumer_and_set_offset(topic: str, timestamp: int):
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'ashege_consumer',
        'session.timeout.ms': 600000,
        'max.poll.interval.ms': 600000,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'
    })
    set_consumer_time_offset(consumer, topic, timestamp)
    return consumer


if __name__ == "__main__":
    BGP_DATA_TOPIC = "ihr_bgp_rrc10_ribs"

    offset_time_string = "2020-08-01T00:00:00"
    offset_datetime = utils.str2dt(offset_time_string, utils.DATETIME_STRING_FORMAT)
    offset_timestamp = utils.dt2ts(offset_datetime)

    ribs_consumer = create_consumer_and_set_offset(BGP_DATA_TOPIC, offset_timestamp)
    for msg in consume_rib_message_at(ribs_consumer, offset_timestamp):
        print(msg)
        break
