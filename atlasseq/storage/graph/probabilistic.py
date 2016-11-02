from atlasseq.storage.base import BaseStorage
from atlasseq.storage.graph.base import BaseGraphStorage
from atlasseq.storage import InMemoryStorage
from atlasseq.storage import RedisStorage
from atlasseq.storage import BerkeleyDBStorage
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

    def getbit(self, i):
        try:
            return self[i]
        except IndexError:
            return False


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

    def update(self, elements, colour):
        [self.add(element, colour) for element in elements]

    def contains(self, element, colour):
        for result in self.hashes(element):
            if self._getbit(colour, result) == 0:
                return False
        return True

    def lookup(self, element, num_elements=None):
        """returns the AND of row of a BloomFilterMatrix corresponding to element"""
        rows = [self._get_row(index, num_elements=num_elements)
                for index in self.hashes(element)]
        bitarray = rows[0]
        if len(rows) > 1:
            for r in rows[:1]:
                bitarray = bitarray & r
        print(element, bitarray)
        return bitarray

    def _setbit(self, colour, index, bit):
        r = self.storage.get_row(index)
        r.setbit(colour, bit)
        self.storage.set_row(index, r)

    def _getbit(self, colour, index):
        return self.storage.get_row(index).getbit(colour)

    def _get_row(self, index, num_elements=None):
        return self.storage.get_row(index, num_elements=num_elements)

    def _set_row(self, index, row):
        return self.storage.set_row(index, row)


class BaseProbabilisticStorage(BaseStorage):

    def __init__(self, config, bloom_filter_size, num_hashes):
        super().__init__(config)
        self.bloomfilter = BloomFilterMatrix(
            size=bloom_filter_size, num_hashes=num_hashes, storage=self)

    def insert(self, kmers, colour):
        """Insert kmer/s into a colour"""
        if isinstance(kmers, list):
            self.bloomfilter.update(kmers, colour)
        else:
            self.bloomfilter.add(kmers, colour)

    def lookup(self, kmers, num_elements=None):
        if isinstance(kmers, list):
            return [self.bloomfilter.lookup(kmer, num_elements=num_elements) for kmer in kmers]
        else:
            return self.bloomfilter.lookup(kmers, num_elements=num_elements)

    def get_row(self, index, num_elements=None):
        b = BitArray()
        b.frombytes(self.get(index, b''))
        if num_elements is None:
            return b
        else:
            # Ensure b is at least num_elements long
            if b.length() < num_elements:
                b.extend([False]*num_elements-b.length())
            assert b.length() >= num_elements
            return b

    def set_row(self, index, b):
        self[index] = b.tobytes()


class ProbabilisticInMemoryStorage(BaseProbabilisticStorage, InMemoryStorage):

    def __init__(self, config={'dict', None}, bloom_filter_size=100000, num_hashes=3):
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabilistic-inmemory'


class ProbabilisticRedisStorage(BaseProbabilisticStorage, RedisStorage):

    def __init__(self, config={"conn": [('localhost', 6379)]}, bloom_filter_size=1000000, num_hashes=3):
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabilistic-redis'


class ProbabilisticBerkeleyDBStorage(BaseProbabilisticStorage, BerkeleyDBStorage):

    def __init__(self, config={'filename': './db'}, bloom_filter_size=1000000, num_hashes=3):
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabilistic-bsddb'
