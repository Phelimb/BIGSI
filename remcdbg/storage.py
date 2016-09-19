from __future__ import print_function
from remcdbg import hash_key
from remcdbg.bytearray import ByteArray

from redispartition import RedisCluster

import os
import json
from sys import getsizeof
try:
    import redis
except ImportError:
    redis = None

try:
    import bsddb3 as bsddb
except ImportError:
    bsddb = None

try:
    import leveldb
except ImportError:
    leveldb = None


def choose_storage(storage_config):
    """ Given the configuration for storage and the index, return the
    configured storage instance.
    """
    if 'dict' in storage_config:
        return InMemoryStorage(storage_config['dict'])
    elif 'redis' in storage_config:
        return RedisStorage(storage_config['redis'])
    elif 'berkeleydb' in storage_config:
        return BerkeleyDBStorage(storage_config['berkeleydb'])
    elif 'rocksdb' in storage_config:
        return RocksDBStorage(storage_config['rocksdb'])
    # elif 'leveldb' in storage_config:
    #     return LevelDBStorage(storage_config['leveldb'])
    else:
        raise ValueError(
            "Only in-memory dictionary, berkeleydb, rocksdb, and redis are supported.")


class BaseStorage(object):

    def __init__(self, config):
        """ An abstract class used as an adapter for storages. """
        raise NotImplementedError

    # def serialize( self, data):
    #     return serialize( data)

    # def deserialize( self, data):
    #     return deserialize( data)

    def keys(self):
        """ Returns a list of binary hashes that are used as dict keys. """
        raise NotImplementedError

    def values(self):
        raise NotImplementedError

    def items(self):
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

    def insert_kmer(self, kmer, colour):
        current_val = self.get(kmer, None)
        ba = ByteArray(byte_array=current_val)
        ba.setbit(colour, 1)
        ba.choose_optimal_encoding(colour)
        self[kmer] = ba.bytes

    def insert_kmers(self, kmers, colour):
        [self.insert_kmer(kmer, colour) for kmer in kmers]

    def get_kmer(self, kmer):
        return self[kmer]

    def get_kmers(self, kmers):
        return [self.get_kmer(k) for k in kmers]

    def count_keys(self):
        raise NotImplementedError


class InMemoryStorage(BaseStorage):

    def __init__(self, config):
        self.name = 'dict'
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

    def __setitem__(self, key, val):
        """ Set `val` at `key`, note that the `val` must be a string. """
        self.storage.__setitem__(key, val)

    def __getitem__(self, key):
        """ Return `val` at `key`, note that the `val` must be a string. """
        return self.storage.__getitem__(key)

    def delete_all(self):
        self.storage = dict()

    def getmemoryusage(self):
        d = self.storage
        size = getsizeof(d)
        size += sum(map(getsizeof, d.values())) + \
            sum(map(getsizeof, d.keys()))
        return size


# class RocksDBStorage(BaseStorage):

#     def __init__(self, config):
#         if 'filename' not in config:
#             raise ValueError(
#                 "You must supply a 'filename' in your config%s" % config)
#         self.db_file = config['filename']
#         try:
#             self.storage = rocksdb.DB(
#                 "test.db", rocksdb.Options(create_if_missing=True))
#         except AttributeError:
#             raise ValueError(
#                 "Please install rocksdb to use rocks DB storage")

#     def keys(self):
#         return self.storage.keys()

#     def __setitem__(self, key, val):
#         if isinstance(key, str):
#             key = str.encode(key)
#         self.storage.put(key, val)

#     def __getitem__(self, key):
#         if isinstance(key, str):
#             key = str.encode(key)
#         self.storage.get(key)

#     def get(self, key, default=None):
#         if isinstance(key, str):
#             key = str.encode(key)
#         try:
#             return self[key]
#         except KeyError:
#             return default

#     def delete_all(self):
#         for k in self.storage.iterkeys():
#             db.delete(k)

#     def getmemoryusage(self):
#         return 0


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

    def __exit__(self, type, value, traceback):
        self.storage.sync()

    def keys(self):
        return self.storage.keys()

    def count_keys(self):
        return len(self.keys())

    def __setitem__(self, key, val):
        if isinstance(key, str):
            key = str.encode(key)
        self.storage[key] = val

    def __getitem__(self, key):
        if isinstance(key, str):
            key = str.encode(key)
        return self.storage[key]

    def get(self, key, default=None):
        if isinstance(key, str):
            key = str.encode(key)
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


def get_vals(r, names, list_of_list_kmers):
    pipe2 = r.pipeline()
    [pipe2.hmget(name, kmers)
     for name, kmers in zip(names, list_of_list_kmers)]
    vals = pipe2.execute()
    return vals


def _batch_insert_redis(conn, hk, colour, count=0):
    r = conn
    with r.pipeline() as pipe:
        try:
            names = [k for k in hk.keys()]
            list_of_list_kmers = [v for v in hk.values()]
            pipe.watch(names)
            vals = get_vals(r, names, list_of_list_kmers)
            pipe.multi()
            for name, current_vals, kmers in zip(names, vals, list_of_list_kmers):
                new_vals = {}
                for j, val in enumerate(current_vals):
                    ba = ByteArray(byte_array=val)
                    ba.setbit(colour, 1)
                    ba.choose_optimal_encoding(colour)
                    new_vals[kmers[j]] = ba.bytes
                pipe.hmset(name, new_vals)
            pipe.execute()
        except redis.WatchError:
            logger.warning("Retrying %s %s " % (r, name))
            if count < 5:
                self._batch_insert(conn, hk, colour, count=count+1)
            else:
                logger.warning(
                    "Failed %s %s. Too many retries. Contining regardless." % (r, name))


class RedisStorage(BaseStorage):

    def __init__(self, config):
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        self.name = 'redis'
        self.storage = RedisCluster([redis.StrictRedis(
            host=host, port=port, db=2) for host, port in config])

    def keys(self, pattern="*"):
        return self.storage.keys(pattern)

    def count_keys(self):
        return self.storage.dbsize()

    def __setitem__(self, key, val):
        if isinstance(key, str):
            key = str.encode(key)
        name = hash_key(key)
        self.storage.hset(name, key, val, partition_arg=1)

    def __getitem__(self, key):
        if isinstance(key, str):
            key = str.encode(key)
        name = hash_key(key)
        return self.storage.hget(name, key, partition_arg=1)

    def delete_all(self):
        self.storage.flushall()

    def getmemoryusage(self):
        return self.storage.calculate_memory()

    def insert_kmers(self, kmers, colour):
        d = self._group_kmers_by_hashkey_and_connection(kmers)
        for conn, hk in d.items():
            _batch_insert_redis(conn, hk, colour)

    def get_kmers(self, kmers):
        kmers = [str.encode(k) if isinstance(k, str) else k for k in kmers]
        names = [hash_key(k) for k in kmers]
        return self.storage.hget(names, kmers, partition_arg=1)

    def _group_kmers_by_hashkey_and_connection(self, kmers):
        d = dict((el, {}) for el in self.storage.connections)
        for k in kmers:
            if isinstance(k, str):
                k = str.encode(k)
            name = hash_key(k)
            conn = self.storage.get_connection(k)
            try:
                d[conn][name].append(k)
            except KeyError:
                d[conn][name] = [k]
        return d
