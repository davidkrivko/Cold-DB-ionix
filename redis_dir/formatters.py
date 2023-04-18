from decimal import Decimal


def convert_payload_datatypes(payload: dict):
    """
    Converts payload dict value to compatible
    with django_redis client
    """
    new_dict = dict()

    for key, value in payload.items():
        if type(value) == Decimal:
            value = str(value)
        new_dict[key] = value

    return new_dict


def decode_redis_hash(raw_hash: dict):
    """
    Converts byte-encoded key-values

    Args:
        raw_hash (dict): [description]

    Returns:
        [dict]: cleaned dictionary
    """

    if len(raw_hash.keys()) == 0:
        return None

    clean_data = dict()

    for key, value in raw_hash.items():
        clean_data[key.decode('utf-8')] = value.decode('utf-8')

    return clean_data
