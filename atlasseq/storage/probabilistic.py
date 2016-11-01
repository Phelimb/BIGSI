from atlasseq.storage.base import BaseStorage
from atlasseq.storage.base import BaseInMemoryStorage
from atlasseq.storage.base import BaseRedisStorage
from atlasseq import hash_key
from atlasseq.bytearray import ByteArray
from redispartition import RedisCluster

import hashlib
# from bitstring import BitArray
from redispartition import RedisCluster
from bitarray import bitarray
import math
import os
import json
from sys import getsizeof
import sys
from HLL import HyperLogLog
import logging
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

from bitarray import bitarray
import mmh3


class BitArray(bitarray):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setbit(self, i, bit):
        if i < 0:
            raise ValueError("Index must be >= 0")
        try:
            self[i] = bit
            return self
        except IndexError:
            self.extend([False]*(1+i-self.length()))
            return self.setbit(i, bit)


class BloomFilterMatrix:

    """Representation of N bloom filters indexed by row"""

    def __init__(self, size, num_hashes, storage):
        self.size = size
        self.num_hashes = num_hashes
        self.storage = storage

    def hash(self, element, seed):
        return mmh3.hash(element, seed) % self.size

    def hashes(self, element):
        for seed in range(self.num_hashes):
            yield self.hash(element, seed)

    def add(self, element, colour):
        for result in self.hashes(element):
            self._setbit(colour, result, 1)

    def contains(self, element, colour):
        for result in self.hashes(element):
            if self._getbit(colour, result) == 0:
                return False
        return True

    def lookup(self, element):
        """returns the AND of row of a BloomFilterMatrix corresponding to element"""
        rows = [self._get_row(index) for index in self.hashes(element)]
        bitarray = rows[0]
        if len(rows) > 1:
            for r in rows[:1]:
                bitarray = bitarray & r
        return bitarray

    def _setbit(self, colour, index, bit):
        r = self.storage.get_row(index)
        r.setbit(colour, bit)
        self.storage.set_row(index, r)

    def _getbit(self, colour, index):
        return self.storage.get_row(index)[colour]

    def _get_row(self, index):
        return self.storage.get_row(index)

    def _set_row(self, index, row):
        return self.storage.set_row(index, row)


class BaseProbabilisticStorage(BaseStorage):

    def __init__(self, config, bloom_filter_size, num_hashes):
        super().__init__(config)
        self.bloomfilter = BloomFilterMatrix(
            size=bloom_filter_size, num_hashes=num_hashes, storage=self)

    def insert(self, kmer, colour):
        """Insert kmer into a colour"""
        self.bloomfilter.add(kmer, colour)

    def lookup(self, kmer):
        return self.bloomfilter.lookup(kmer)

    def get_row(self, index):
        b = BitArray()
        b.frombytes(self.get(index, b''))
        return b

    def set_row(self, index, b):
        assert b.tobytes() is not None
        self[index] = b.tobytes()


class ProbabilisticInMemoryStorage(BaseProbabilisticStorage, BaseInMemoryStorage):

    def __init__(self, config={'dict', None}, bloom_filter_size=100000, num_hashes=3):
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabilistic-inmemory'


class ProbabilisticRedisStorage(BaseProbabilisticStorage, BaseRedisStorage):

    def __init__(self, config={"conn": [('localhost', 6379)]}, bloom_filter_size=1000000, num_hashes=3):
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabibistic-redis'

    # # def get_name(self, key):
    # #     if isinstance(key, str):
    # #         hkey = str.encode(key)
    # #     elif isinstance(key, int):
    # #         hkey = (key).to_bytes(4, byteorder='big')
    # #     name = hash_key(hkey)
    # #     return name

    # # def __setitem__(self, key, val):
    # #     name = self.get_name(key)
    # #     self.storage.hset(name, key, val, partition_arg=1)

    # # def __getitem__(self, key):
    # #     name = self.get_name(key)
    # #     return self.storage.hget(name, key, partition_arg=1)

    # def delete_all(self):
    #     self.storage.flushall()

    # def getmemoryusage(self):
    #     return self.storage.calculate_memory()

    # def insert_kmers(self, kmers, colour):
    #     assert self.num_hashes == 3
    #     all_hashes = self._kmers_to_hash_indexes(kmers)
    #     names = self._key_names_from_hashes(all_hashes)
    #     hk = self._group_kmers_by_hashkey_and_connection(all_hashes)
    #     for conn, names_hashes in hk.items():
    #         names = [k for k in names_hashes.keys()]
    #         hashes = [hs for hs in names_hashes.values()]
    #         _batch_insert_prob_redis(
    #             conn, names, hashes, colour, self.bloom_filter_size)

    # def _group_kmers_by_hashkey_and_connection(self, all_hashes):
    #     d = dict((el, {}) for el in self.storage.connections)
    #     for k in all_hashes:
    #         name = self.get_name(k)
    #         conn = self.storage.get_connection(k)
    #         try:
    #             d[conn][name].append(k)
    #         except KeyError:
    #             d[conn][name] = [k]
    #     return d

    # def _kmers_to_hash_indexes(self, kmers):
    #     kmers = [str.encode(kmer) if isinstance(
    #         kmer, str) else kmer for kmer in kmers]
    #     all_hashes = [
    #         int(hashlib.sha1(kmer).hexdigest(), 16) % self.bloom_filter_size for kmer in kmers]
    #     all_hashes.extend(
    #         [int(hashlib.sha256(kmer).hexdigest(), 16) % self.bloom_filter_size for kmer in kmers])
    #     all_hashes.extend(
    #         [int(hashlib.sha384(kmer).hexdigest(), 16) % self.bloom_filter_size for kmer in kmers])
    #     return all_hashes

    # def _key_names_from_hashes(self, hash_indexes):
    #     return [self.get_name(key) for key in hash_indexes]

    # def get_kmers(self, kmers):
    #     all_hashes = self._kmers_to_hash_indexes(kmers)
    #     names = self._key_names_from_hashes(all_hashes)
    #     primary_colour_presence = self.storage.hget(
    #         names, all_hashes, partition_arg=1)
    #     return primary_colour_presence

    # def dump(self, raw=False):
    #     for k in self.storage.scan_iter('*'):
    #         for k2, v in self.storage.hgetall(k).items():
    #             if raw:
    #                 print(v)
    #             else:
    #                 ba = BitArray()
    #                 ba.frombytes(v)
    #                 print("\t".join([str(int(k2)), ba.to01()]))

    # def bitcount(self):
    #     for k in self.storage.scan_iter('*'):
    #         for k2, v in self.storage.hgetall(k).items():
    #             ba = BitArray()
    #             ba.frombytes(v)
    #             sys.stdout.write("".join([str(ba.count()), " "]))

    # def get_bloom_filter(self, primary_colour):
    #     bf = BitArray()
    #     names = [self.get_name(key) for key in range(self.bloom_filter_size)]
    #     vals = self.storage.hget(
    #         names, range(self.bloom_filter_size), partition_arg=1)
    #     for i, v in enumerate(vals):
    #         v = self.get(i, 0)
    #         j = False
    #         if v:
    #             ba = ByteArray(byte_array=v)
    #             j = bool(ba.getbit(primary_colour))
    #         bf.append(j)
    #     return bf

    # def add_to_kmers_count(self, kmers, sample):
    #     self.metadata.pfadd('kmer_count_%s' % sample, *kmers)
    #     self.metadata.pfadd('kmer_count', *kmers)

    # def count_kmers(self, sample):
    #     if sample is None:
    #         return self.metadata.pfcount('kmer_count')
    #     else:
    #         return self.metadata.pfcount('kmer_count_%s' % sample)

    # def kmer_union(self, sample1, sample2):
    #     samples = ['kmer_count_%s' % sample1, 'kmer_count_%s' % sample2]
    #     self.metadata.pfcount(*samples)
    #     return self.metadata.pfcount(*samples)


# def get_vals(r, names, list_of_list_kmers):
#     pipe2 = r.pipeline()
#     [pipe2.hmget(name, kmers)
#      for name, kmers in zip(names, list_of_list_kmers)]
#     vals = pipe2.execute()
#     return vals


# def hget_vals(r, names, list_of_list_kmers):
#     pipe2 = r.pipeline()
#     [pipe2.hget(name, kmers)
#      for name, kmers in zip(names, list_of_list_kmers)]
#     vals = pipe2.execute()
#     return vals


# def _batch_insert_prob_redis(conn, names, all_hashes, colour, bloom_filter_size, count=0):
#     r = conn
#     with r.pipeline() as pipe:
#         try:
#             pipe.watch(names)
#             vals = get_vals(r, names, all_hashes)
#             pipe.multi()
#             for name, values, hs in zip(names, vals, all_hashes):
#                 for val, h in zip(values, hs):
#                     ba = ByteArray(byte_array=val)
#                     ba.setbit(colour, 1)
#                     # ba.choose_optimal_encoding(colour)
#                     pipe.hset(name, h, ba.bytes)
#             pipe.execute()
#         except redis.WatchError:
#             logger.warning("Retrying %s %s " % (r, name))
#             if count < 5:
#                 self._batch_insert(conn, hk, colour, count=count+1)
#             else:
#                 logger.warning(
#                     "Failed %s %s. Too many retries. Contining regardless." % (r, name))


# def _batch_insert_redis(conn, hk, colour, count=0):
#     r = conn
#     with r.pipeline() as pipe:
#         try:
#             names = [k for k in hk.keys()]
#             list_of_list_kmers = [v for v in hk.values()]
#             pipe.watch(names)
#             vals = get_vals(r, names, list_of_list_kmers)
#             pipe.multi()
#             for name, current_vals, kmers in zip(names, vals, list_of_list_kmers):
#                 new_vals = {}
#                 for j, val in enumerate(current_vals):
#                     ba = ByteArray(byte_array=val)
#                     ba.setbit(colour, 1)
#                     ba.choose_optimal_encoding(colour)
#                     new_vals[kmers[j]] = ba.bytes
#                 pipe.hmset(name, new_vals)
#             pipe.execute()
#         except redis.WatchError:
#             logger.warning("Retrying %s %s " % (r, name))
#             if count < 5:
#                 self._batch_insert(conn, hk, colour, count=count+1)
#             else:
#                 logger.warning(
#                     "Failed %s %s. Too many retries. Contining regardless." % (r, name))
