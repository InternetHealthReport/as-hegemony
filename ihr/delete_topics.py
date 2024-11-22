from confluent_kafka.admin import AdminClient
import sys

def delete_topics(a, topics):
    """ delete topics """

    # Call delete_topics to asynchronously delete topics, a future is returned.
    # By default this operation on the broker returns immediately while
    # topics are deleted in the background. But here we give it some time (30s)
    # to propagate in the cluster before returning.
    #
    # Returns a dict of <topic,future>.
    fs = a.delete_topics(topics, operation_timeout=30)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))


if __name__ == '__main__':
    admin = AdminClient({'bootstrap.servers': 'kafka1:9092'})
    suffix = sys.argv[1]
    topics = []
    collectors = [
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

    prefix = [
            f'ihr_bgp_atom{suffix}_', f'ihr_bgp_atom_meta{suffix}_',
            f'ihr_bcscore{suffix}_', f'ihr_bcscore_meta{suffix}_',
            ]

    for p in prefix:
        for c in collectors:
            topics.append(p+c)

    topics.append(f'ihr_hegemony{suffix}')
    topics.append(f'ihr_hegemony_meta{suffix}')

    delete_topics(admin, topics)
