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


def remove_path_prepending(prepended_aspath: list):
    prev = ""
    aspath = list()
    for asn in prepended_aspath:
        if asn == prev:
            continue
        aspath.append(asn)
        prev = asn
    return aspath


def is_ip_v6(prefix: str):
    return ":" in prefix
