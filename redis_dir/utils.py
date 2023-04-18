import datetime

import pytz

from redis_dir.daos import redis_dao


DEVICE_ONLINE_STATUS_DELTA_SEC = 30
dao = redis_dao

utc_tz = pytz.timezone('UTC')


def fetch_online_status_from_online_stream(timestamp) -> object:
    """
    Searches serial number in redis_dir stream of devices online status
    and returns bool response in a context object

    Returns:
        object: [data] (bool) True if online, else False
    """

    ctx = {
        'detail': None,
        'data': None, 
    }

    time = datetime.datetime.now(utc_tz)
    timestamp_str = timestamp
    timestamp = datetime.datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f%z")
    delta = time - timestamp

    if delta.seconds <= DEVICE_ONLINE_STATUS_DELTA_SEC:
        ctx['data'] = True
        ctx['detail'] = f"Timedelta (last seen online) [{delta.seconds} s]"
    else:
        ctx['data'] = False
        ctx['detail'] = f"Timedelta (last seen online) [{delta.seconds} s] " \
                        f"status exceeds limit [{DEVICE_ONLINE_STATUS_DELTA_SEC} s]"

    return ctx


def fetch_device_data_from_data_stream(sn: str, device: str) -> object:
    """Searches serial number in redis_dir stream of devices data
    and returns payload in response object

    Args:
        sn (str): device serial number
        device (str): device type (TRM - thermostat, ION - controller)

    Returns:
        object: ctx[data] device related data
    """

    ctx = {
        'detail': None,
        'data': None, 
    }
    if device == "TRM":
        record = dao.get_thermostat_from_data_stream(sn)
    else:
        record = dao.get_ioniq_from_data_stream(sn)
   
    if len(record) == 0:
        ctx['detail'] = 'No records found for the serial specified'
        ctx['data'] = False
        return ctx

    payload = dict()
    binary_dict = record[0][1]
    
    for key, value in binary_dict.items():
        payload[key.decode('utf-8')] = value.decode('utf-8')

    ctx['data'] = payload
    return ctx
