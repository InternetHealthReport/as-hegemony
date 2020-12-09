from functools import partial
import logging
import msgpack

from kafkadata import create_topic, prepare_producer


class DataProducer:
    def __init__(self, data_builder):
        self.data_builder = data_builder
        self.kafka_data_topic = data_builder.kafka_data_topic
        self.kafka_meta_data_topic = data_builder.kafka_meta_data_topic
        self.messages_per_peer = dict()

    def produce_kafka_messages_between(self):
        create_topic(self.kafka_data_topic)
        create_topic(self.kafka_meta_data_topic)

        start_timestamp = self.data_builder.start_timestamp
        end_timestamp = self.data_builder.end_timestamp

        logging.debug(f"start dumping bcscore to {self.kafka_data_topic}, "
                      f"between {start_timestamp} and {end_timestamp}")

        for timestamp, data_generator in self.data_builder.consume_and_calculate():
            producer = prepare_producer()
            self.produce_kafka_messages_at(producer, data_generator, timestamp)

        logging.debug(f"successfully dumped bgpatom: ({self.kafka_data_topic}, {start_timestamp} - {end_timestamp})")

    def produce_kafka_messages_at(self, producer, data_generator, timestamp: int):
        logging.debug(f"({self.kafka_data_topic}, {timestamp}): start producing ...")

        self.produce_kafka_data_at(producer, data_generator, timestamp)
        self.produce_kafka_meta_data_at(producer, timestamp)

        logging.debug(f"({self.kafka_data_topic}, {timestamp}): DONE")

    def produce_kafka_data_at(self, producer, data_generator, timestamp: int):
        ms_timestamp = timestamp * 1000

        for message, peer_address in data_generator:
            delivery_report = partial(self.__delivery_report, peer_address)
            producer.produce(
                self.kafka_data_topic,
                msgpack.packb(message, use_bin_type=True),
                peer_address,
                callback=delivery_report,
                timestamp=ms_timestamp
            )
            producer.poll(0)
        producer.flush()

    def produce_kafka_meta_data_at(self, producer, timestamp: int):
        ms_timestamp = timestamp * 1000

        kafka_meta_message = {
            "messages_per_peer": self.messages_per_peer,
            "timestamp": timestamp
        }
        producer.produce(
            self.kafka_meta_data_topic,
            msgpack.packb(kafka_meta_message, use_bin_type=True),
            callback=self.__meta_delivery_report,
            timestamp=ms_timestamp
        )
        producer.flush()

    def __delivery_report(self, peer_address, err, _):
        if err is not None:
            logging.error('Message delivery failed: {}'.format(err))
        else:
            if peer_address not in self.messages_per_peer:
                self.messages_per_peer[peer_address] = 0
            self.messages_per_peer[peer_address] += 1

    @staticmethod
    def __meta_delivery_report(err, _):
        if err is not None:
            logging.error('metadata delivery failed: {}'.format(err))
        else:
            pass
