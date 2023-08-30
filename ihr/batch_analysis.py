# Should be run from the directory containing produce_*.py scripts
# e.g. python3 ihr/daily-run.py 2021-01-11
# If no argument given then compute AS hegemony for the current day
import os
import sys
from subprocess import Popen
import time
import arrow
import json
import msgpack 
from confluent_kafka import Consumer, TopicPartition, KafkaError


slow_start = 600 # lag for starting bcscore and hegemony code

BOOTSTRAP_SERVER = 'kafka1:9092'
DATE_FMT = '%Y-%m-%dT%H:%M:00'
all_collectors = [
        'route-views.sydney', 'route-views.chicago',
        'route-views2', 
        'route-views3', 
        'route-views4', 
        'route-views5', 
        'route-views.linx',
        'route-views.rio',
        'route-views.sg',
        'route-views.napafrica', 
        'rrc00',
        'rrc01',
        'rrc04', 
        'rrc06', 
        'rrc10', 
        'rrc11',
        'rrc12', 
        'rrc13',  # FIXME 
        'rrc14',
        'rrc15', 
        'rrc16',  # slow updates?
#        'rrc19', # unavailable?
        'rrc20', 
        'rrc23', 
        'rrc24',
        'rrc25',
        'rrc26',
        ]

selected_collectors = []


def select_collectors(start_time):
    print('Selecting collectors with updated data...')
    selection = []
    start_threshold = start_time.shift(hours=-2)
    end_threshold = start_time.shift(hours=2)
    for collector in all_collectors:
        topic = 'ihr_bgp_%s_ribs' % collector

        # Instantiate Kafka Consumer
        consumer = Consumer({
            'bootstrap.servers': BOOTSTRAP_SERVER,
            'group.id': 'ihr_hegemony_rib_selection',
            'enable.auto.commit': False,
            })
        partition = TopicPartition(topic, 0, int(start_threshold.timestamp())*1000)

        try:
            time_offset = consumer.offsets_for_times( [partition] , timeout = 60)
            # consumer.seek_to_end()
            # offset = consumer.position(partition)-1
        except Exception as e:
            print(collector, "'s kafka RIB topic inexistant!")
            consumer.close()
            continue

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


if len(sys.argv) < 2:
    sys.exit(f'usage: {sys.argv[0]} all|atom|bcscore|hege|prefix ip_version config_file [start_time [end_time]]')

analysis_type = sys.argv[1]
ip_version = sys.argv[2]
config_file = sys.argv[3]

config = json.load(open(config_file, 'r'))

# Remove collectors with no IPv6
if ip_version == '6':
    all_collectors.remove('route-views2')

# read start/end dates from command line
end_time = None
if len(sys.argv) > 4:
    start_time = arrow.get(sys.argv[4])
    if len(sys.argv) > 5:
        end_time = arrow.get(sys.argv[5])
else:
    start_time = arrow.now().replace(hour=0, minute=0, second=0)

# set default end time if not given
if end_time is None:
    if 'prefix' in analysis_type:
        end_time= start_time.shift(minutes=15)
    else:
        end_time= start_time.shift(days=1)

start_str = start_time.strftime(DATE_FMT)
end_str = end_time.strftime(DATE_FMT)

print('start: ', start_str, 'end: ', end_str)

# Find collectors that are up-to-date
selected_collectors = select_collectors(start_time)
childs = []

if 'all' in analysis_type or 'atom' in analysis_type:
    # Produce BGP atoms for each collector
    for collector in selected_collectors: 
        print('# BGP atoms', collector, start_str, end_str)
        if ip_version == '4':
            childs.append(Popen(['python3', '../produce_bgpatom.py', '-C', config_file, '-c', collector, '-s', start_str, '-e', end_str]))
            time.sleep(1)
        else:
            print('Info: using BGP atoms from IPv4 script')

    if ip_version == '4':
        time.sleep(slow_start)

if 'all' in analysis_type or 'bcscore' in analysis_type:
    # Produce BC scores for each collector
    for collector in selected_collectors: 
        print('# Betweenness Centrality', collector, start_str, end_str)
        childs.append(Popen(['python3', '../produce_bcscore.py', '-C', config_file, '-c', collector, '-s', start_str, '-e', end_str, '--ip_version', ip_version]))
        time.sleep(1)

    time.sleep(slow_start)

if 'all' in analysis_type or 'hege' in analysis_type:
    # Produce AS Hegemony scores 
    print('# AS Hegemony')
    #os.system('python3 produce_hege.py -s %s -e %s -c %s ' % 
    #     ( start_str, end_str, ','.join(selected_collectors)) )
    for i in range(config['kafka']['default_topic_config']['num_partitions']):
        childs.append(Popen(['python3', '../produce_hege.py', '-C', config_file, '-s', start_str, '-e', end_str, '--partition_id', str(i), '-c', ','.join(selected_collectors) ]) )
        time.sleep(1)

if 'prefix' in analysis_type:
    # Produce BC scores for each collector
    for collector in selected_collectors: 
        print('# Betweenness Centrality', collector, start_str, end_str)
        childs.append(Popen(['python3', '../produce_bcscore.py', '-C', config_file, '-c', collector, '-s', start_str, '-e', end_str, '--ip_version', ip_version, '-p']))
        time.sleep(1)

    time.sleep(slow_start)

    for i in range(config['kafka']['default_topic_config']['num_partitions']):
        print("# AS Hegemony (partition %s)" % i)
        hege_child = Popen(['python3', '../produce_hege.py', '-C', config_file, '-s', start_str, '-e', end_str, '--partition_id', str(i), '-c', ','.join(selected_collectors), '-p' ])
        if i % 2 == 0: # Run two instances in parrallel
            hege_child.wait()


# Wait for completion
for child in childs:
    child.wait()

