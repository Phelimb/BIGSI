from atlasseq.storage.base import BaseStorage
from atlasseq.storage.graph.base import BaseGraphStorage
from atlasseq.storage import InMemoryStorage
from atlasseq.storage import RedisStorage
from atlasseq.storage import SimpleRedisStorage
from atlasseq.storage import BerkeleyDBStorage
from atlasseq import hash_key
from atlasseq.bytearray import ByteArray
from atlasseq.bitarray import BitArray
from redispartition import RedisCluster

import hashlib
# from bitstring import BitArray
from redispartition import RedisCluster
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

import mmh3


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
        for index in self.hashes(element):
            self._setbit(index, colour, 1)

    def update(self, elements, colour):
        indexes = []
        for element in elements:
            indexes.extend(self.hashes(element))
        self._setbits(indexes, colour, 1)

    def contains(self, element, colour):
        for index in self.hashes(element):
            if self._getbit(index, colour) == 0:
                return False
        return True

    def lookup(self, element, num_elements=None):
        """returns the AND of row of a BloomFilterMatrix corresponding to element"""
        if isinstance(element, list):
            return self._lookup_elements(element, num_elements)
        else:
            return self._lookup_element(element, num_elements)

    def _lookup_elements(self, elements, num_elements=None):
        indexes = []
        for e in elements:
            indexes.extend([h for h in self.hashes(e)])
        rows = self._get_rows(indexes, num_elements)
        bas = []
        for i in range(0, len(rows), self.num_hashes):
            bas.append(self._binary_and(rows[i:i + self.num_hashes]))
        return bas

    def _lookup_element(self, element, num_elements=None):
        indexes = self.hashes(element)
        rows = self._get_rows(indexes, num_elements)
        return self._binary_and(rows)

    def _binary_and(self, rows):
        bitarray = rows[0]
        if len(rows) > 1:
            for r in rows[:1]:
                bitarray = bitarray & r
        return bitarray

    def _setbit(self, index, colour, bit):
        self.storage.setbit(index, colour, bit)

    def _setbits(self, indexes, colour, bit):
        self.storage.setbits(indexes, colour, bit)

    def _getbit(self, index, colour):
        return self.storage.getbit(index, colour)

    def _get_row(self, index, num_elements=None):
        return self.storage.get_row(index, num_elements=num_elements)

    def _get_rows(self, indexes, num_elements=None):
        return self.storage.get_rows(indexes, num_elements=num_elements)

    def get_column(self, colour):
        bf = BitArray()
        for i in range(self.size):
            bf.extend([self._getbit(i, colour)])
        return bf


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

    def lookup(self, kmer, num_elements=None):
        return self.bloomfilter.lookup(kmer, num_elements=num_elements)

    def get_bloom_filter(self, colour):
        return self.bloomfilter.get_column(colour)

    def setbit(self, index, colour, bit):
        r = self.get_row(index)
        r.setbit(colour, bit)
        self.set_row(index, r)

    def getbit(self, index, colour):
        return self.get_row(index).getbit(colour)

    def get_row(self, index, num_elements=None):
        b = BitArray()
        b.frombytes(self.get(index, b''))
        return self._check_num_elements(b, num_elements)

    def _check_num_elements(self, b, num_elements):
        if num_elements is None:
            return b[:num_elements]
        else:
            # Ensure b is at least num_elements long
            if b.length() < num_elements:
                b.extend([False]*(num_elements-b.length()))
            assert b.length() >= num_elements
            return b[:num_elements]

    def get_rows(self, indexes, num_elements=None):
        return [self.get_row(i, num_elements) for i in indexes]

    def set_row(self, index, b):
        self[index] = b.tobytes()


class ProbabilisticInMemoryStorage(BaseProbabilisticStorage, InMemoryStorage):

    def __init__(self, config={'dict', None}, bloom_filter_size=100000, num_hashes=3):
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabilistic-inmemory'

    def setbits(self, indexes, colour, bit):
        for index in indexes:
            self.setbit(index, colour, bit)


class ProbabilisticRedisStorage(BaseProbabilisticStorage, RedisStorage):

    def __init__(self, config={"conn": [('localhost', 6379)]}, bloom_filter_size=1000000, num_hashes=3):
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabilistic-redis'

    def get_rows(self, indexes, num_elements=None):
        indexes = [i for i in indexes]
        bas = []
        rows = self._get_raw_rows(indexes, num_elements)
        for r in rows:
            b = BitArray()
            if r is None:
                r = b''
            b.frombytes(r)
            bas.append(self._check_num_elements(b, num_elements))
        return bas

    def _get_raw_rows(self, indexes, num_elements):
        names = [self.get_name(i) for i in indexes]
        return self.storage.hget(names, indexes, partition_arg=1)


class ProbabilisticBerkeleyDBStorage(BaseProbabilisticStorage, BerkeleyDBStorage):

    def __init__(self, config={'filename': './db'}, bloom_filter_size=1000000, num_hashes=3):
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabilistic-bsddb'

    def setbits(self, indexes, colour, bit):
        for index in indexes:
            self.setbit(index, colour, bit)
