## Prefix-hege

The purpose of this project is to calculate hegemony value for any AS or prefix globally reachable.

### `produce_bgpatom`
- running script
```commandline
python3 produce_bgpatom.py -c rrc00 -s 2020-08-01T00:00:00 -e 2020-08-01T01:00:00
python3 produce_bgpatom.py -c rrc10 -s 2020-08-01T00:00:00 -e 2020-08-01T01:00:00
python3 produce_bgpatom.py -c route-views2 -s 2020-08-01T00:00:00 -e 2020-08-01T01:00:00
python3 produce_bgpatom.py -c route-views.linx -s 2020-08-01T00:00:00 -e 2020-08-01T01:00:00
```
- produce compressed bgp data, message format
```json
{
    "prefixes": "a batch of tuple (prefix, origin_as)",
    "aspath": "as_path",
    "peer_address": "address of peer",
    "timestamp": "timestamp"
}
```

###  `produce_bscore`
- running script for producing **AS** hegemony score
```commandline
python3 produce_bcscore.py -c rrc00 -s 2020-08-01T00:00:00 -e 2020-08-01T01:00:00
python3 produce_bcscore.py -c rrc10 -s 2020-08-01T00:00:00 -e 2020-08-01T01:00:00
python3 produce_bcscore.py -c route-views2 -s 2020-08-01T00:00:00 -e 2020-08-01T01:00:00
python3 produce_bcscore.py -c route-views.linx -s 2020-08-01T00:00:00 -e 2020-08-01T01:00:00
```
- running script for producing **Prefix** hegemony score
```commandline
python3 produce_bcscore.py -p -s 2020-08-01T00:00:00 -e 2020-08-01T00:15:00 -c rrc00
 ```
- produce AS's between-ness centrality score
```json
{
    "bcscore": "bcscore",
    "scope": "asn scope",
    "peer_address": "address of peer",
    "peer_asn": "asn of peer",
    "collector": "collector",
    "timestamp": "timestamp"
}
```

### `produce_hege`
- running script for producing **AS** hegemony score
```commandline
python3 produce_hege.py -s 2020-08-01T00:00:00 -e 2020-08-01T00:15:00 -c rrc00,rrc10,route-views.linx,route-views2
 ```
- running script for producing **Prefix** hegemony score
```commandline
python3 produce_hege.py -p -s 2020-08-01T00:00:00 -e 2020-08-01T00:15:00 -c rrc00,rrc10,route-views.linx,route-views2
 ```

### `hege_loader.py`
- running script to get data from a specific scope
```commandline
python3 -m hege.hegemony.hege_loader -t 2020-08-01T00:00:00 -s as15169
python3 -m hege.hegemony.hege_loader -t 2020-08-01T00:00:00 -s 8.8.8.8
```

### config.json
```json
{
  "kafka": {
    "bootstrap_servers": "kafka:9092",
    "no_new_message_limit": 60, // consumer wait 1 minute between 2 messages
    "leader_wait_minutes": 60, // consumer wait 1 hour for offset timestamp to be available 
    "default_topic_config": 
  }
}
```
