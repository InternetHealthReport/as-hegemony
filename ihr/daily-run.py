# Should be run from the directory containing produce_*.py scripts
# e.g. python3 ihr/daily-run.py 2021-01-11
# If no argument given then compute AS hegemony for the current day
import os
import sys
from subprocess import Popen
import time
import arrow
import msgpack 
from confluent_kafka import Consumer, TopicPartition, KafkaError


slow_start = 600 # lag for starting bcscore and hegemony code

NB_PARTITION = 4 # this should be greater or equal to the number of partition set in config.json
BOOTSTRAP_SERVER = 'kafka1:9092'
DATE_FMT = '%Y-%m-%dT00:00:00'
all_collectors = [
        'route-views.sydney', 'route-views.chicago',
        'route-views2', 'route-views.linx',
        'route-views.jinx', 
        'rrc00',
        'rrc04', 'rrc10', 'rrc11',
        'rrc12', 
       # 'rrc13',  # FIXME 
        'rrc14',
        'rrc15', 'rrc16', 'rrc19',
        'rrc20', 'rrc23', 'rrc24'
        ]

selected_collectors = []


def select_collectors(start_time):
    selection = []
    start_threshold = start_time.shift(hours=-2)
    end_threshold = start_time.shift(hours=2)
    for collector in all_collectors:
        topic = 'ihr_bgp_%s_ribs' % collector

        # Instantiate Kafka Consumer
        consumer = Consumer({
            'bootstrap.servers': BOOTSTRAP_SERVER,
            'group.id': 'ihr_rib_selection',
            'enable.auto.commit': False,
            })
        partition = TopicPartition(topic, 0, start_threshold.timestamp*1000)

        time_offset = consumer.offsets_for_times( [partition] )
        # consumer.seek_to_end()
        # offset = consumer.position(partition)-1

        if time_offset[0].offset == -1: 
            print(collector, ' ignored! ')
            consumer.close()
            continue
        else:
            consumer.assign(time_offset)
            # consumer.seek(partition, offset)

        date = None
        # retrieve messages
        kafka_msg = consumer.poll()

        if kafka_msg is not None and not kafka_msg.error():
            ts = kafka_msg.timestamp()
            date = arrow.get(ts[1])

        if date is not None and date > start_threshold and date < end_threshold:
            selection.append(collector)
            print(collector, ' ', date) 
        else:
            print(collector, ' ignored! ')

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
print('python3 produce_hege.py -s %s -e %s -c %s ' % 
    ( start_str, end_str, ' '.join(selected_collectors)) )
#os.system('python3 produce_hege.py -s %s -e %s -c %s ' % 
#     ( start_str, end_str, ','.join(selected_collectors)) )
childs = []
for i in range(NB_PARTITION):
    childs.append(Popen(['python3', 'produce_hege.py', '-s', start_str, '-e', end_str, '--partition_id', str(i), '-c', ','.join(selected_collectors) ]) )

# Wait for completion
for child in childs:
    child.wait()

