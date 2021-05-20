import logging
import json
import time
import sys

import msgpack
import confluent_kafka
from confluent_kafka import Consumer, TopicPartition, KafkaError
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic, Producer, KafkaException

from hege.utils.config import Config

KAFKA_BOOTSTRAP_SERVERS = Config.get("kafka")["bootstrap_servers"]
NO_NEW_MESSAGE_LIMIT = Config.get("kafka")["no_new_message_limit"]
LEADER_WAIT_MINUTES = Config.get("kafka")["leader_wait_minutes"]
DEFAULT_TOPIC_CONFIG = Config.get("kafka")["default_topic_config"]


def create_consumer_and_set_offset(topic: str, timestamp: int, partition_id=None):
    wait_for_leader_count = 0
    while wait_for_leader_count < LEADER_WAIT_MINUTES:
        try:
            consumer = Consumer({
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': f'ashege_consumer_{timestamp}',
                'session.timeout.ms': 600000,
                'max.poll.interval.ms': 600000,
                'fetch.min.bytes': 100000,
                'enable.auto.commit': False,
                'auto.offset.reset': 'earliest'
            })
#            consumer.subscribe([topic])

            timestamp_ms = timestamp * 1000
            partitions = []
            if partition_id is None:
                topic_info = consumer.list_topics(topic)
                partitions = [TopicPartition(topic, partition_id, timestamp_ms) 
                        for partition_id in  topic_info.topics[topic].partitions.keys()]
            else: 
                partitions = [TopicPartition(topic, int(partition_id), timestamp_ms)]

            time_offset = consumer.offsets_for_times( partitions )

            ready = False

            for partition in time_offset:
                if partition.offset != -1:
                    ready = True

            if not ready:
                consumer.close()
                wait_for_leader_count += 1
                time.sleep(60)
                logging.warning(f"waiting for new data: {time_offset}")
                continue

            consumer.assign(time_offset)
            # make sure the consumer is ready to fetch
            # consumer.poll()
            # set offsets
            # for offset in time_offset:
                # consumer.seek(offset)

            #consumer.assign(time_offset)
            logging.info(f"successfully assign consumer to {topic}, time offset at {timestamp}")
            return consumer

        except KafkaException as ke:
            if ke.args[0].code() == KafkaError.LEADER_NOT_AVAILABLE:
                consumer.close()
                wait_for_leader_count += 1
                time.sleep(60)
                continue
            else:
                logging.error("KafkaException: " + str(ke))
                raise Exception("cannot assign topic partition")

        except Exception as e:
            logging.error(e)
            raise e


def consume_stream(consumer: Consumer, timebin: int):
    if consumer is None:
        logging.error(f"trying to get data from aborted consumer at {timebin}")
        return 

    # Read non-empty partitions
    partitions = [part for part in consumer.assignment() if part.offset>=0]
    nb_partitions = len(partitions)
    nb_stopped_partitions = 0
    timebin_ms = timebin*1000

    while True:
        kafka_msg = consumer.poll(NO_NEW_MESSAGE_LIMIT)

        if kafka_msg is None:
            logging.error(f"consumer timeout")
            return

        if kafka_msg.error():
            logging.error(f"consumer error {kafka_msg.error()}")
            continue
        # Filter with start and end times
        ts = kafka_msg.timestamp()

        if ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME and ts[1] > timebin_ms:
            consumer.pause([TopicPartition(kafka_msg.topic(), kafka_msg.partition())])
            nb_stopped_partitions += 1
            if nb_stopped_partitions < nb_partitions:
                continue
            else:
                return

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
        'queue.buffering.max.messages': 10000000,
        'queue.buffering.max.kbytes': 2097151,
        'linger.ms': 200,
        'batch.num.messages': 1000000,
        'default.topic.config': {
            'compression.codec': 'lz4',
            'acks': 1,
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

    elif command == "clean":
        for c in debug_collectors:
            delete_topic([f"ihr_bgp_atom_{c}", f"ihr_bgp_atom_meta_{c}"])
            delete_topic([f"ihr_bcscore_{c}", f"ihr_bcscore_meta_{c}"])
            delete_topic([f"ihr_bcscore_prefix_{c}", f"ihr_bcscore_prefix_meta_{c}"])
        delete_topic(["ihr_hegemony", "ihr_hegemony_meta"])
        delete_topic(["ihr_prefix_hegemony", "ihr_prefix_hegemony_meta"])
