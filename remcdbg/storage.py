from __future__ import print_function
from remcdbg import hash_key
from remcdbg.bytearray import ByteArray
import hashlib
from bitstring import BitArray
from redispartition import RedisCluster
from bitarray import bitarray
import math
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
    elif 'probabilistic-inmemory' in storage_config:
        return ProbabilisticInMemoryStorage(storage_config['probabilistic-inmemory'])
    elif 'probabilistic-redis' in storage_config:
        return ProbabilisticRedisStorage(storage_config['probabilistic-redis'])
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


def byte_to_bitstring(byte):
    a = str("{0:b}".format(byte))
    if len(a) < 8:
        a = "".join(['0'*(8-len(a)), a])
    return a


def setbit(bytes, i):
    a = bitarray()
    a.frombytes(bytes)
    try:
        a[i] = 1
        return a.tobytes()
    except IndexError:
        a = bitarray()
        _bytes = b"".join(
            [bytes, b'\x00']*math.ceil(float(1+i-len(bytes)*8)/8))
        return setbit(_bytes, i)


class ProbabilisticStorage(BaseStorage):

    def insert_kmer(self, kmer, colour):
        assert self.num_hashes == 2
        if isinstance(kmer, str):
            kmer = str.encode(kmer)

        hashes = [int(hashlib.sha1(kmer).hexdigest(), 16) % self.array_size, int(
            hashlib.sha256(kmer).hexdigest(), 16) % self.array_size,
            int(hashlib.sha384(kmer).hexdigest(), 16) % self.array_size
        ]
        for h in hashes:
            val = self.get(h, None)
            ba = ByteArray(byte_array=val)
            ba.setbit(colour, 1)
            ba.choose_optimal_encoding(colour)
            self[h] = ba.bytes

    def get_kmer(self, kmer):
        assert self.num_hashes == 2
        if isinstance(kmer, str):
            kmer = str.encode(kmer)
        hashes = [int(hashlib.sha1(kmer).hexdigest(), 16) % self.array_size, int(
            hashlib.sha256(kmer).hexdigest(), 16) % self.array_size,
            int(hashlib.sha384(kmer).hexdigest(), 16) % self.array_size
        ]
        b1 = self[hashes[0]]
        b2 = self[hashes[1]]
        b3 = self[hashes[2]]

        if b1 is None:
            b1 = b'\x00'
        if b2 is None:
            b2 = b'\x00'
        if b3 is None:
            b3 = b'\x00'
        ba1 = ByteArray(byte_array=b1)
        ba2 = ByteArray(byte_array=b1)
        ba3 = ByteArray(byte_array=b1)
        ba1.to_dense()
        ba2.to_dense()
        ba3.to_dense()

        # print('get', kmer, hashes, v1, v2, (v1 & v2).tobytes())

        return b"".join([b'\x00', (ba1.bitstring & ba2.bitstring & ba3.bitstring).tobytes()])


class ProbabilisticInMemoryStorage(ProbabilisticStorage):

    def __init__(self, config):
        self.name = 'probabilistic-inmemory'
        self.array_size = config.get('array_size', 5000000)
        self.num_hashes = config.get('num_hashes', 2)
        self.storage = [None]*self.array_size

    def keys(self):
        """ Returns a list of binary hashes that are used as dict keys. """
        return NotImplementedError("Probabilistic storage doesn't store keys (only the hash of them)")

    def count_keys(self):
        return len(self.storage.keys())

    def values(self):
        return NotImplementedError("Probabilistic storage doesn't store keys (only the hash of them)")

    def items(self):
        return NotImplementedError("Probabilistic storage doesn't store keys (only the hash of them)")

    def insert_kmers(self, kmers, colour):
        assert self.num_hashes == 2
        [self.insert_kmer(kmer, colour) for kmer in kmers]

    def get_kmers(self, kmers):
        assert self.num_hashes == 2
        return [self.get_kmer(k) for k in kmers]

    def __setitem__(self, key, val):
        """ Set `val` at `key`, note that the `val` must be a string. """
        self.storage[key] = val

    def __getitem__(self, key):
        """ Return `val` at `key`, note that the `val` must be a string. """
        return self.storage[key]

    def delete_all(self):
        self.storage = [None]*self.array_size

    def getmemoryusage(self):
        d = self.storage
        size = getsizeof(d)
        return size


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
        elif isinstance(key, int):
            key = str.encode(str(key))
        name = hash_key(key)
        self.storage.hset(name, key, val, partition_arg=1)

    def __getitem__(self, key):
        if isinstance(key, str):
            key = str.encode(key)
        elif isinstance(key, int):
            key = str.encode(str(key))
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


class ProbabilisticRedisStorage(ProbabilisticStorage):

    def __init__(self, config):
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        self.name = 'probabibistic-redis'
        self.array_size = config['array_size']
        self.num_hashes = config['num_hashes']
        self.storage = RedisCluster([redis.StrictRedis(
            host=host, port=port, db=2) for host, port in config['conn']])

    def keys(self, pattern="*"):
        return self.storage.keys(pattern)

    def count_keys(self):
        return self.storage.dbsize()

    def get_name(self, key):
        if isinstance(key, str):
            hkey = str.encode(key)
        elif isinstance(key, int):
            hkey = (key).to_bytes(4, byteorder='big')
        name = hash_key(hkey)
        return name

    def __setitem__(self, key, val):
        name = self.get_name(key)
        self.storage.hset(name, key, val, partition_arg=1)

    def __getitem__(self, key):
        name = self.get_name(key)
        return self.storage.hget(name, key, partition_arg=1)

    def delete_all(self):
        self.storage.flushall()

    def getmemoryusage(self):
        return self.storage.calculate_memory()

    def insert_kmers(self, kmers, colour):
        assert self.num_hashes == 2
        all_hashes = self._kmers_to_hash_indexes(kmers)
        names = self._key_names_from_hashes(all_hashes)
        hk = self._group_kmers_by_hashkey_and_connection(all_hashes)
        # print(hk)
        for conn, names_hashes in hk.items():
            names = [k for k in names_hashes.keys()]
            hashes = [hs for hs in names_hashes.values()]
            _batch_insert_prob_redis(
                conn, names, hashes, colour, self.array_size)
        # [self.insert_kmer(kmer, colour) for kmer in kmers]
        # print(names, all_hashes)

    def _group_kmers_by_hashkey_and_connection(self, all_hashes):
        d = dict((el, {}) for el in self.storage.connections)
        for k in all_hashes:
            name = self.get_name(k)
            conn = self.storage.get_connection(k)
            try:
                d[conn][name].append(k)
            except KeyError:
                d[conn][name] = [k]
        return d

    # def get_kmers(self, kmers):
    #     assert self.num_hashes == 2
    #     return [self.get_kmer(k) for k in kmers]

    def _kmers_to_hash_indexes(self, kmers):
        kmers = [str.encode(kmer) if isinstance(
            kmer, str) else kmer for kmer in kmers]
        all_hashes = [
            int(hashlib.sha1(kmer).hexdigest(), 16) % self.array_size for kmer in kmers]
        all_hashes.extend(
            [int(hashlib.sha256(kmer).hexdigest(), 16) % self.array_size for kmer in kmers])
        all_hashes.extend(
            [int(hashlib.sha384(kmer).hexdigest(), 16) % self.array_size for kmer in kmers])
        return all_hashes

    def _key_names_from_hashes(self, hash_indexes):
        return [self.get_name(key) for key in hash_indexes]

    def get_kmers(self, kmers):
        all_hashes = self._kmers_to_hash_indexes(kmers)
        names = self._key_names_from_hashes(all_hashes)
        return self.storage.hget(names, all_hashes, partition_arg=1)

    def dump(self):
        for k in self.storage.scan_iter('*'):
            for k2, v in self.storage.hgetall(k).items():
                ba = bitarray()
                ba.frombytes(v)
                print("\t".join([str(int(k2)), ba.to01()]))


def get_vals(r, names, list_of_list_kmers):
    pipe2 = r.pipeline()
    [pipe2.hmget(name, kmers)
     for name, kmers in zip(names, list_of_list_kmers)]
    vals = pipe2.execute()
    return vals


def hget_vals(r, names, list_of_list_kmers):
    pipe2 = r.pipeline()
    [pipe2.hget(name, kmers)
     for name, kmers in zip(names, list_of_list_kmers)]
    vals = pipe2.execute()
    return vals


def _batch_insert_prob_redis(conn, names, all_hashes, colour, array_size, count=0):
    r = conn
    with r.pipeline() as pipe:
        try:
            pipe.watch(names)
            vals = get_vals(r, names, all_hashes)
            pipe.multi()
            for name, values, hs in zip(names, vals, all_hashes):
                for val, h in zip(values, hs):
                    ba = ByteArray(byte_array=val)
                    ba.setbit(colour, 1)
                    ba.choose_optimal_encoding(colour)
                    pipe.hset(name, h, ba.bytes)
            pipe.execute()
        except redis.WatchError:
            logger.warning("Retrying %s %s " % (r, name))
            if count < 5:
                self._batch_insert(conn, hk, colour, count=count+1)
            else:
                logger.warning(
                    "Failed %s %s. Too many retries. Contining regardless." % (r, name))


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
