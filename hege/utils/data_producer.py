from functools import partial
import logging
import msgpack

from hege.utils.kafka_data import create_topic, prepare_producer

META_DATA_KEY_LIMIT = 50


class DataProducer:
    def __init__(self, data_builder):
        self.data_builder = data_builder
        self.kafka_data_topic = data_builder.kafka_data_topic
        self.kafka_meta_data_topic = data_builder.kafka_meta_data_topic
        self.messages_per_key = dict()

    def produce_kafka_messages_between(self):
        create_topic(self.kafka_data_topic)
        create_topic(self.kafka_meta_data_topic)

        start_timestamp = self.data_builder.start_timestamp
        end_timestamp = self.data_builder.end_timestamp

        logging.debug(f"start dumping to {self.kafka_data_topic}, "
                      f"between {start_timestamp} and {end_timestamp}")

        for timestamp, data_generator in self.data_builder.consume_and_calculate():
            producer = prepare_producer()
            self.produce_kafka_messages_at(producer, data_generator, timestamp)

        logging.debug(f"successfully dumped: ({self.kafka_data_topic}, {start_timestamp} - {end_timestamp})")

    def produce_kafka_messages_at(self, producer, data_generator, timestamp: int):
        logging.debug(f"({self.kafka_data_topic}, {timestamp}): start producing ...")

        self.messages_per_key = dict()
        self.produce_kafka_data_at(producer, data_generator, timestamp)
        self.produce_kafka_meta_data_at(producer, timestamp)

        logging.debug(f"({self.kafka_data_topic}, {timestamp}): DONE")

    def produce_kafka_data_at(self, producer, data_generator, timestamp: int):
        ms_timestamp = timestamp * 1000
        for message, key in data_generator:
            try:
                delivery_report = partial(self.__delivery_report, key)
                producer.produce(
                    self.kafka_data_topic,
                    msgpack.packb(message, use_bin_type=True),
                    key,
                    callback=delivery_report,
                    timestamp=ms_timestamp
                )
                producer.poll(0)

            except BufferError:
                logging.warning('buffer error, the queue must be full! Flushing...')
                producer.flush()

                logging.info('queue flushed, try re-write previous message')
                producer.produce(
                    self.kafka_data_topic,
                    msgpack.packb(message, use_bin_type=True),
                    key,
                    callback=delivery_report,
                    timestamp=ms_timestamp
                )
                producer.poll(0)

        producer.flush()

    def produce_kafka_meta_data_at(self, producer, timestamp: int):
        ms_timestamp = timestamp * 1000
        for meta_data in self.produce_kafka_meta_data_helper():
            kafka_meta_message = {
                "messages_per_peer": meta_data,
                "timestamp": timestamp
            }
            try:
                producer.produce(
                    self.kafka_meta_data_topic,
                    msgpack.packb(kafka_meta_message, use_bin_type=True),
                    callback=self.__meta_delivery_report,
                    timestamp=ms_timestamp
                )
                producer.poll(0)

            except BufferError:
                logging.warning('buffer error, the queue must be full! Flushing...')
                producer.flush()

                logging.info('queue flushed, try re-write previous message')
                producer.produce(
                    self.kafka_meta_data_topic,
                    msgpack.packb(kafka_meta_message, use_bin_type=True),
                    callback=self.__meta_delivery_report,
                    timestamp=ms_timestamp
                )
                producer.poll(0)
        producer.flush()

    def produce_kafka_meta_data_helper(self):
        partial_meta_data = dict()
        for key in self.messages_per_key:
            partial_meta_data[key] = self.messages_per_key[key]
            if len(partial_meta_data) > META_DATA_KEY_LIMIT:
                yield partial_meta_data
                partial_meta_data = dict()

        if partial_meta_data:
            yield partial_meta_data

    def __delivery_report(self, key, err, _):
        if err is not None:
            logging.error('message delivery failed: {}'.format(err))
        else:
            if key not in self.messages_per_key:
                self.messages_per_key[key] = 0
            self.messages_per_key[key] += 1

    @staticmethod
    def __meta_delivery_report(err, _):
        if err is not None:
            logging.error('metadata delivery failed: {}'.format(err))
        else:
            pass
