## Prefix-hege

The purpose of this project is to calculate hegemony value for wild prefixes.

### `produce_bgpatom`
- running script
```python3
python3 produce_bgpatom.py -c rrc10 -s 2020-08-01T00:00:00 -e 2020-08-01T01:00:00
```
- produce compressed bgp data, message format
```json
{
    "prefixes": a batch of tuple (prefix, origin_as),
    "aspath": as_path,
    "peer_address": address of peer,
    "timestamp": timestamp
}
```