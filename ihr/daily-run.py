# Should be run from the directory containing produce_*.py scripts
# e.g. python3 ihr/daily-run.py 2021-01-11
# If no argument given then compute AS hegemony for the current day
import os
import sys
from subprocess import Popen
import time
import arrow
import msgpack 
from kafka import KafkaConsumer
from kafka.structs import TopicPartition


slow_start = 300 # timer to start bcscore (wait for bgpatom to complete)

BOOTSTRAP_SERVER = 'kafka1:9092'
DATE_FMT = '%Y-%m-%dT00:00:00'
all_collectors = [
        'route-views.sydney', 'route-views.chicago',
        'route-views2', 'route-views.linx',
        'route-views.jinx', 'route-views.linx',
        'rrc00',
        'rrc04', 'rrc10', 'rrc11',
        'rrc12', 'rrc13', 'rrc14',
        'rrc15', 'rrc16', 'rrc19',
        'rrc20', 'rrc23', 'rrc24'
        ]

selected_collectors = []


def select_collectors(start_time):
    selection = []
    start_threshold = start_time.shift(hours=-2)
    for collector in all_collectors:
        topic = 'ihr_bgp_%s_ribs' % collector

        # Instantiate Kafka Consumer
        consumer = KafkaConsumer(topic, bootstrap_servers=[BOOTSTRAP_SERVER],
                group_id='ihr_rib_selection', enable_auto_commit=False, 
                consumer_timeout_ms=10000,
                value_deserializer=lambda v: msgpack.unpackb(v, raw=False))
        partition = TopicPartition(topic, 0)

        # go to end of the stream with dummy poll
        consumer.poll()
        consumer.seek_to_end()
        offset = consumer.position(partition)-1

        if offset<0: 
            print(collector, ' empty topic!!')
        else:
            consumer.seek(partition, offset)

        date = None
        # retrieve messages
        for i, message in enumerate(consumer):
            date = arrow.get(message.timestamp)
            print(collector, ' ', date) 
            break

        if date is not None and date > start_threshold:
            selection.append(collector)

        consumer.close()

    return selection

# Set start/end dates
if len(sys.argv) > 1:
    start_time = arrow.get(sys.argv[1])
else:
    start_time = arrow.now().replace(hour=0, minute=0, second=0)
end_time= start_time.shift(days=1)

start_str = start_time.strftime(DATE_FMT)
end_str = end_time.strftime(DATE_FMT)


# Find collectors that are up-to-date
selected_collectors = select_collectors(start_time)

# Produce BGP atoms for each collector
for collector in selected_collectors: 
    print('# BGP atoms', collector, start_str, end_str)
    Popen(['python3', 'produce_bgpatom.py', '-c', collector, '-s', start_str, '-e', end_str])

time.sleep(slow_start)

# Produce BC scores for each collector
for collector in selected_collectors: 
    print('# Betweenness Centrality', collector, start_str, end_str)
    Popen(['python3', 'produce_bcscore.py', '-c', collector, '-s', start_str, '-e', end_str])

time.sleep(slow_start)

# Produce AS Hegemony scores 
print('# AS Hegemony')
# print('python3 produce_hege.py -c %s -s %s -e %s' % 
    # (' '.join(collectors), start_str, end_str) )
for i in range(4):
    Popen(['python3', 'produce_hege.py', '-s', start_str, '-e', end_str, '-c', ','.join(selected_collectors) ]) 

