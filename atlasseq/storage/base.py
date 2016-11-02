import redis
import sys
import os
from redispartition import RedisCluster
from atlasseq import hash_key

try:
    import bsddb3 as bsddb
except ImportError:
    bsddb = None


class BaseStorage(object):

    def __init__(self, config):
        """ An abstract class used as an adapter for storages. """
        raise NotImplementedError

    def __setitem__(self, key, val):
        """ Set `val` at `key`, note that the `val` must be a string. """
        raise NotImplementedError

    def __getitem__(self, key):
        """ Return `val` at `key`, note that the `val` must be a string. """
        raise NotImplementedError

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def delete_all(self):
        raise NotImplementedError

    def incr(self, key):
        raise NotImplementedError


class InMemoryStorage(BaseStorage):

    def __init__(self, config):
        self.name = 'dict'
        self.storage = dict()

    def __setitem__(self, key, val):
        """ Set `val` at `key`, note that the `val` must be a string. """
        self.storage.__setitem__(key, val)

    def __getitem__(self, key):
        """ Return `val` at `key`, note that the `val` must be a string. """
        return self.storage.__getitem__(key)

    def incr(self, key):
        if self.get(key) is None:
            self[key] = 0
        v = self.get(key)
        v += 1
        self[key] = v

    def delete_all(self):
        self.storage = dict()

    def keys(self):
        """ Returns a list of binary hashes that are used as dict keys. """
        return self.storage.keys()

    def count_keys(self):
        return len(self.storage)

    def values(self):
        return self.storage.values()

    def items(self):
        return self.storage.items()

    def getmemoryusage(self):
        d = self.storage
        size = getsizeof(d)
        size += sum(map(getsizeof, d.values())) + \
            sum(map(getsizeof, d.keys()))
        return size


class BaseRedisStorage(BaseStorage):

    def __init__(self):
        pass

    def incr(self, key):
        return self.storage.incr(key)

    def get(self, key, default=None):
        try:
            v = self[key]
            if v is None:
                return default
            else:
                return self[key]
        except KeyError:
            return default

    def keys(self, pattern="*"):
        return self.storage.keys(pattern)

    def count_keys(self):
        return self.storage.dbsize()

    def delete_all(self):
        self.storage.flushall()

    def getmemoryusage(self):
        return self.storage.calculate_memory()


class SimpleRedisStorage(BaseRedisStorage):

    def __init__(self, config):
        super().__init__()
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        self.name = 'redis'
        host, port, db = config['conn'][0]
        self.storage = redis.StrictRedis(
            host=host, port=port, db=int(db))

    def __setitem__(self, key, val):
        self.storage.set(key, val)

    def __getitem__(self, key):
        return self.storage.get(key)

    def setbit(self, index, colour, bit):
        self.storage.setbit(index, colour, bit)

    def getbit(self, index, colour):
        return self.storage.getbit(index, colour)


class RedisStorage(BaseRedisStorage):

    def __init__(self, config):
        super().__init__()
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        self.name = 'redis'
        self.redis_cluster = True
        self.storage = RedisCluster([redis.StrictRedis(
            host=host, port=port, db=int(db)) for host, port, db in config['conn']])

    def __setitem__(self, key, val):
        name = self.get_name(key)
        self.storage.hset(name, key, val, partition_arg=1)

    def __getitem__(self, key):
        name = self.get_name(key)
        return self.storage.hget(name, key, partition_arg=1)

    def setbit(self, index, colour, bit):
        name = self.get_name(index)
        self.storage.setbit(name, colour, bit)

    def getbit(self, index, colour):
        name = self.get_name(index)
        return self.storage.getbit(name, colour)

    def get_name(self, key):
        if isinstance(key, str):
            hkey = str.encode(key)
        elif isinstance(key, int):
            hkey = (key).to_bytes(4, byteorder='big')
        name = hash_key(hkey)
        return name


class BerkeleyDBStorage(BaseStorage):

    def __init__(self, config):
        if 'filename' not in config:
            raise ValueError(
                "You must supply a 'filename' in your config%s" % config)
        self.db_file = config['filename']
        try:
            self.storage = bsddb.hashopen(self.db_file)
        except AttributeError:
            raise ValueError(
                "Please install bsddb3 to use berkeley DB storage")

    def incr(self, key):
        if self.get(key) is None:
            self[key] = 0
        v = int(self.get(key))
        v += 1
        self[key] = v

    def __exit__(self, type, value, traceback):
        self.storage.sync()

    def keys(self):
        return self.storage.keys()

    def count_keys(self):
        return len(self.keys())

    def __setitem__(self, key, val):
        if isinstance(key, str):
            key = str.encode(key)
        elif isinstance(key, int):
            key = str.encode(str(key))
        if isinstance(val, str):
            val = str.encode(val)
        elif isinstance(val, int):
            val = str.encode(str(val))
        self.storage[key] = val

    def __getitem__(self, key):
        if isinstance(key, str):
            key = str.encode(key)
        elif isinstance(key, int):
            key = str.encode(str(key))
        return self.storage[key]

    def get(self, key, default=None):
        if isinstance(key, str):
            key = str.encode(key)
        elif isinstance(key, int):
            key = str.encode(str(key))
        try:
            return self[key]
        except KeyError:
            return default

    def delete_all(self):
        self.storage.close()
        os.remove(self.db_file)
        self.storage = bsddb.hashopen(self.db_file)

    def getmemoryusage(self):
        return 0
