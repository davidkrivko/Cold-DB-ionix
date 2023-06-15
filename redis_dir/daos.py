import datetime
import redis

from redis_dir.schemas import StreamsKeySchema
from redis_dir.formatters import (
    decode_redis_hash,
)


now = datetime.datetime.now()

streams_key_schema = StreamsKeySchema()

url = "rediss://default:AVNS_D7tNG5W4o3c7IiXu5tF@db-redis-nyc1-75696-do-user-13548984-0.b.db.ondigitalocean.com:25061/1"
connect = redis.from_url(url)


class RedisDao:
    """
    Instantiates Data Access object which uses
    redis_dir to store iot data in its data types (streams, ts etc.)
    """

    MAXLEN = 86400  # max amount of records in a stream (oldest trimed)

    def get_paired_relay_controller_data(self, sn: str, connection):
        key = streams_key_schema.paired_relay_key(sn)
        raw_data = connection.hgetall(key)
        data = decode_redis_hash(raw_data)
        return data

    def get_paired_thermostat_data(self, sn: str, connection):
        key = streams_key_schema.paired_thermostat_key(sn)
        raw_data = connection.hgetall(key)
        data = decode_redis_hash(raw_data)
        return data

    def get_receiver_data(self, sn: str, connection):
        key = streams_key_schema.receiver_key(sn)
        raw_data = connection.hgetall(key)
        data = decode_redis_hash(raw_data)
        return data


redis_dao = RedisDao()
