import logging
import json
import time
import sys

import msgpack
from confluent_kafka import Consumer, TopicPartition, KafkaError
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic, Producer, KafkaException


with open("/app/config.json", "r") as f:
    config = json.load(f)
KAFKA_BOOTSTRAP_SERVERS = config["kafka"]["bootstrap_servers"]
NO_NEW_MESSAGE_LIMIT = config["kafka"]["no_new_message_limit"]
LEADER_WAIT_MINUTES = config["kafka"]["leader_wait_minutes"]
DEFAULT_TOPIC_CONFIG = config["kafka"]["default_topic_config"]


def create_consumer_and_set_offset(topic: str, timestamp: int):
    wait_for_leader_count = 0
    while wait_for_leader_count < LEADER_WAIT_MINUTES:
        try:
            consumer = Consumer({
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': 'ashege_consumer',
                'session.timeout.ms': 600000,
                'max.poll.interval.ms': 600000,
                'enable.auto.commit': False,
                'auto.offset.reset': 'earliest'
            })

            timestamp_ms = timestamp * 1000
            time_offset = consumer.offsets_for_times(
                [TopicPartition(
                    topic,
                    partition=0,
                    offset=timestamp_ms
                )], timeout=1)

            if time_offset == -1:
                consumer.close()
                wait_for_leader_count += 1
                time.sleep(60)
                continue

            consumer.assign(time_offset)
            logging.info(f"successfully assign consumer to {topic}, time offset at {timestamp}")
            return consumer

        except KafkaException as ke:
            if ke.args[0].code() == KafkaError.LEADER_NOT_AVAILABLE:
                consumer.close()
                wait_for_leader_count += 1
                time.sleep(60)
                continue
            else:
                logging.error("KafkaException: " + ke)
                raise Exception("cannot assign topic partition")

        except Exception as e:
            logging.error(e)
            raise e


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

        number_of_empty_message = 0
        message = msgpack.unpackb(kafka_msg.value(), raw=False)
        yield message, kafka_msg


def create_topic(topic_name: str, topic_config=DEFAULT_TOPIC_CONFIG):
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    topic_list = [NewTopic(topic_name, **topic_config)]
    created_topic = admin_client.create_topics(topic_list)

    for topic, future in created_topic.items():
        try:
            future.result()
            logging.warning("Topic {} created".format(topic))
        except Exception as e:
            logging.warning("Failed to create topic {}: {}".format(topic, e))


def prepare_producer():
    logging.debug("prepare producer")
    producer = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'default.topic.config': {
            'compression.codec': 'snappy'
        }
    })
    return producer


def delete_topic(topics_list: list):
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    deleted_topics = admin_client.delete_topics(topics_list)
    for topic, future in deleted_topics.items():
        try:
            future.result()
            logging.warning("Topic {} deleted".format(topic))
        except Exception as e:
            logging.warning("Failed to delete topic {}: {}".format(topic, e))


if __name__ == "__main__":
    if len(sys.argv) < 1:
        sys.exit(0)

    debug_collectors = ["route-views2", "route-views.linx", "rrc00", "rrc10"]
    command = sys.argv[1]
    if command == "delete-topic":
        topics_name = list(map(lambda x: x.strip(), sys.argv[2].split(",")))
        delete_topic(topics_name)

    elif command == "delete-atom":
        for c in debug_collectors:
            delete_topic([f"ihr_bgp_atom_{c}", f"ihr_bgp_atom_meta_{c}"])

    elif command == "delete-bc-asn":
        for c in debug_collectors:
            delete_topic([f"ihr_bcscore_{c}", f"ihr_bcscore_meta_{c}"])

    elif command == "delete-bc-prefix":
        for c in debug_collectors:
            delete_topic([f"ihr_bcscore_prefix_{c}", f"ihr_bcscore_prefix_meta_{c}"])

    elif command == "delete-hege-asn":
        delete_topic(["ihr_hegemony", "ihr_hegemony_meta"])

    elif command == "delete-hege-prefix":
        delete_topic(["ihr_prefix_hegemony", "ihr_prefix_hegemony_meta"])
