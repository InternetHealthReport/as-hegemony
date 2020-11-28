import datetime

DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%S"


def dt2ts(dt):
    if type(dt) == int:
        return dt
    elif type(dt) == str:
        dt = str2dt(dt)
    return int((dt - datetime.datetime(1970, 1, 1)).total_seconds())


def str2dt(s, fmt="%Y-%m-%d %H:%M:%S %Z"):
    if type(s) == int:
        return ts2dt(s)
    return datetime.datetime.strptime(s, fmt)


def ts2dt(ts):
    return datetime.datetime.fromtimestamp(ts + datetime.datetime(1970, 1, 1).timestamp())


def remove_path_prepending(prepended_aspath: list):
    prev = ""
    aspath = list()
    for asn in prepended_aspath:
        if asn == prev:
            continue
        aspath.append(asn)
        prev = asn
    return aspath
