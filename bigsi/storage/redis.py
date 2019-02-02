from bigsi.storage.base import BaseStorage
from bigsi.constants import DEFAULT_REDIS_STORAGE_CONFIG
import redis
from bitarray import bitarray
from bigsi.utils import batch


class RedisStorage(BaseStorage):
    def __init__(self, storage_config=None):
        if storage_config is None:
            storage_config = DEFAULT_REDIS_STORAGE_CONFIG
        self.storage_config = storage_config
        self.storage = redis.StrictRedis(**storage_config)
        self.pipe = self.storage.pipeline()
        self.write_batch_size = int(self.storage_config.get("write_batch_size", 10000))

    def __repr__(self):
        return "redis Storage"

    def delete_all(self):
        return self.storage.flushall()

    def __execute_pipeline(self):
        result = self.pipe.execute()
        self.pipe = self.storage.pipeline()
        return result

    def batch_set(self, keys, values):
        for batchiter in batch(zip(keys, values), self.write_batch_size):
            for k, v in batchiter:
                self.pipe.set(k, v)
            self.__execute_pipeline()

    def batch_get(self, keys):
        for k in keys:
            self.pipe.get(k)
        return self.__execute_pipeline()

    def set_bit(self, key, pos, bit):
        _key = self.convert_to_bitarray_key(key)
        self.storage.setbit(_key, pos, bit)

    def get_bit(self, key, pos):
        _key = self.convert_to_bitarray_key(key)
        return bool(self.storage.getbit(_key, pos))

    def incr(self, key):
        __key = self.convert_to_integer_key(key)
        return self.storage.incr(__key)
