from confluent_kafka.admin import AdminClient

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
    admin = AdminClient({'bootstrap.servers': 'kafka3'})
    topics = []
    collectors = [
        'route-views.sydney', 'route-views.chicago',
        'route-views2', 'route-views.linx',
        'rrc00',
        'rrc04', 'rrc10', 'rrc11',
        'rrc12', 'rrc13', 'rrc14',
        'rrc15', 'rrc16', 'rrc19',
        'rrc20', 'rrc23', 'rrc24'
        ]

    prefix = [
            'ihr_bgp_atom_', 'ihr_bgp_atom_meta_',
            'ihr_bcscore_', 'ihr_bcscore_meta_',
            ]

    for p in prefix:
        for c in collectors:
            topics.append(p+c)

    topics.append('ihr_hegemony')
    topics.append('ihr_hegemony_meta')

    delete_topics(admin, topics)
