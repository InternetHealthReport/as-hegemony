import datetime

DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%S"


def datetime_to_timestamp(dt):
    if type(dt) == int:
        return dt
    elif type(dt) == str:
        dt = str_datetime_to_datetime(dt)
    return int((dt - datetime.datetime(1970, 1, 1)).total_seconds())


def str_datetime_to_datetime(str_dt, fmt=DATETIME_STRING_FORMAT):
    return datetime.datetime.strptime(str_dt, fmt)


def timestamp_to_datetime(ts):
    return datetime.datetime.fromtimestamp(ts + datetime.datetime(1970, 1, 1).timestamp())


def str_datetime_to_timestamp(str_dt: str, fmt=DATETIME_STRING_FORMAT):
    dt = str_datetime_to_datetime(str_dt, fmt)
    return datetime_to_timestamp(dt)


def deduplicate_as_path(aspath: list):
    """Remove duplicate ASNs from the path and return the remaining path in order.

    Primary purpose is to remove path prepending, but also handle cases of "invalid" AS
    paths of the form A B A, which are rare but exist.
    """

    # Cut end of the path if origin AS appears multiple times
    origin_as = aspath[-1]
    if origin_as in aspath[:-1]:
        first_idx = aspath.index(origin_as)
        aspath = aspath[:first_idx+1]

    # Remove deduplicated ASes
    seen_asns = set()
    dedup_aspath = list()
    for asn in aspath:
        if asn in seen_asns:
            continue
        dedup_aspath.append(asn)
        seen_asns.add(asn)
    return dedup_aspath


def is_ip_v6(prefix: str):
    return ":" in prefix
