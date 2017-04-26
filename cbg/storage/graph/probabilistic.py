from cbg.storage.base import BaseStorage
from cbg.storage.graph.base import BaseGraphStorage
from cbg.storage import InMemoryStorage
from cbg.storage import RedisHashStorage
from cbg.storage import RedisBitArrayStorage
from cbg.storage import SimpleRedisStorage
from cbg.storage import BerkeleyDBStorage
from cbg.utils import hash_key
from cbg.bytearray import ByteArray
from cbg.bitvector import BitArray
from bitarray import bitarray
import hashlib
# from bitstring import BitArray
import math
import os
import json
from sys import getsizeof
import sys
from HLL import HyperLogLog
import logging
import time
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)
from cbg.utils import DEFAULT_LOGGING_LEVEL
from cbg.utils import chunks
logger.setLevel(DEFAULT_LOGGING_LEVEL)

try:
    import redis
except ImportError:
    redis = None


import mmh3


class BloomFilterMatrix:

    """Representation of N bloom filters indexed by row"""

    def __init__(self, size, num_hashes, storage):
        self.size = size
        self.num_hashes = num_hashes
        self.storage = storage

    def hash(self, element, seed):
        _hash = mmh3.hash(element, seed) % self.size
        return _hash

    def hashes(self, element):
        for seed in range(self.num_hashes):
            h = self.hash(element, seed)
            yield h

    def add(self, element, colour):
        for index in self.hashes(element):
            self._setbit(index, colour, 1)

    def add_column(self, bloomfilter, colour):
        colour = int(colour)
        for i, j in enumerate(bloomfilter):
            self._setbit(i, colour, j)

    def update(self, elements, colour):
        indexes = self._get_all_indexes(elements)
        self._setbits(indexes, colour, 1)

    def create(self, elements):
        start = time.time()
        bloomfilter = bitarray(self.size)
        for e in elements:
            for i in self.hashes(e):
                bloomfilter[i] = True
        end = time.time()
        logger.debug("Created bloom filter in %i seconds" % (end-start))
        return bloomfilter

    def _get_all_indexes(self, elements):
        start = time.time()
        indexes = set()
        for element in elements:
            indexes.update(self.hashes(element))
        end = time.time()
        logger.debug("Generated %i hashes for %i elements in %i seconds" % (
            len(indexes), len(elements), end-start))
        return indexes

    def contains(self, element, colour):
        for index in self.hashes(element):
            if self._getbit(index, colour) == 0:
                return False
        return True

    def lookup(self, element):
        """returns the AND of row of a BloomFilterMatrix corresponding to element"""
        if isinstance(element, list):
            return self._lookup_elements(element)
        else:
            return self._lookup_element(element)

    def _lookup_elements(self, elements):
        indexes = []
        for e in elements:
            indexes.extend([h for h in self.hashes(e)])
        rows = self._get_rows(indexes)
        bas = []
        for i in range(0, len(rows), self.num_hashes):
            bas.append(self._binary_and(rows[i:i + self.num_hashes]))
        return bas

    def _lookup_element(self, element):
        indexes = self.hashes(element)
        rows = self._get_rows(indexes)
        return self._binary_and(rows)

    def _binary_and(self, rows):
        assert len(rows) > 0
        bitarray = rows[0]
        if len(rows) > 1:
            for r in rows[1:]:
                bitarray = bitarray & r
        return bitarray

    def _setbit(self, index, colour, bit):
        self.storage.setbit(index, colour, bit)

    def _setbits(self, indexes, colour, bit):
        self.storage.setbits(indexes, colour, bit)

    def _getbit(self, index, colour):
        return self.storage.getbit(index, colour)

    def _get_row(self, index):
        return self.storage.get_row(index)

    def _get_rows(self, indexes):
        return self.storage.get_rows(indexes)

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

    def set_bloom_filter_size(self, bloom_filter_size):
        self.bloomfilter.size = bloom_filter_size

    def set_num_hashes(self, num_hashes):
        self.bloomfilter.num_hashes = num_hashes

    def insert(self, bf, colour):
        """Insert bloomfilter into a colour"""
        self.bloomfilter.add_column(bf, colour)

    def lookup(self, kmer):
        return self.bloomfilter.lookup(kmer)

    def lookup_all_present(self, elements):
        if not elements:
            raise ValueError(
                "You're trying to lookup a null element is your sequence search shorter than the kmer size?")
        indexes = []
        for e in elements:
            indexes.extend([h for h in self.bloomfilter.hashes(e)])
        rows = self.get_rows(indexes)
        return self.bloomfilter._binary_and(rows)

    def get_bloom_filter(self, colour):
        return self.bloomfilter.get_column(colour)

    def create_bloom_filter(self, kmers):
        return self.bloomfilter.create(kmers)

    def get_row(self, index):
        b = BitArray()
        b.frombytes(self.get(index, b''))
        return b

    def get_rows(self, indexes):
        return [self.get_row(i) for i in indexes]

    def set_row(self, index, b):
        self[index] = b.tobytes()

    def items(self):
        for i in range(self.bloomfilter.size):
            yield (i, self.get(i, b''))

    def dump(self, outfile, num_colours):
        for indices in chunks(range(self.bloomfilter.size), min(self.bloomfilter.size, 10000)):
            vs = self.get_rows(indices)
            for v in vs:
                outfile.write(v.tobytes())

    def load(self, infile, num_colours):
        record_size = math.ceil(num_colours / 8)
        for i in range(self.bloomfilter.size):
            self[i] = infile.read(record_size)


class ProbabilisticInMemoryStorage(BaseProbabilisticStorage, InMemoryStorage):

    def __init__(self, config={'dict', None}, bloom_filter_size=100000, num_hashes=3):
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabilistic-inmemory'

    def setbits(self, indexes, colour, bit):
        for index in indexes:
            self.setbit(index, colour, bit)

    def setbit(self, index, colour, bit):
        r = self.get_row(index)
        r.setbit(colour, bit)
        self.set_row(index, r)

    def getbit(self, index, colour):
        return self.get_row(index).getbit(colour)


class ProbabilisticRedisHashStorage(BaseProbabilisticStorage, RedisBitArrayStorage):

    def __init__(self, config={"conn": [('localhost', 6379)]}, bloom_filter_size=1000000, num_hashes=3):
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabilistic-redis'

    def get_rows(self, indexes):
        indexes = [i for i in indexes]
        bas = []
        rows = self._get_raw_rows(indexes)
        for r in rows:
            b = BitArray()
            if r is None:
                r = b''
            b.frombytes(r)
            bas.append(b)
        return bas

    def _get_raw_rows(self, indexes):
        names = [self.get_name(i) for i in indexes]
        return self.storage.hget(names, indexes, partition_arg=1)

    def setbit(self, index, colour, bit):
        r = self.get_row(index)
        r.setbit(colour, bit)
        self.set_row(index, r)

    def getbit(self, index, colour):
        return self.get_row(index).getbit(colour)


class ProbabilisticRedisBitArrayStorage(BaseProbabilisticStorage, RedisBitArrayStorage):

    def __init__(self, config={"conn": [('localhost', 6379)]}, bloom_filter_size=1000000, num_hashes=3):
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabilistic-redis'

    def get_rows(self, indexes):
        indexes = list(indexes)
        bas = []
        rows = self._get_raw_rows(indexes)
        for r in rows:
            b = BitArray()
            if r is None:
                b.append(False)
            else:
                b.frombytes(r)
            bas.append(b)
        return bas

    def _get_raw_rows(self, indexes):
        pipe = self.storage.pipeline()
        for i in indexes:
            pipe.get(i)
        raw_rows = pipe.execute()
        return raw_rows

    def items(self):
        for i, r in enumerate(self._get_raw_rows(range(self.bloomfilter.size))):
            if r is None:
                r = b''
            yield (i, r)


class ProbabilisticBerkeleyDBStorage(BaseProbabilisticStorage, BerkeleyDBStorage):

    def __init__(self, config={'filename': './db'}, bloom_filter_size=1000000, num_hashes=3):
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabilistic-bsddb'

    def setbits(self, indexes, colour, bit):
        for index in indexes:
            self.setbit(index, colour, bit)

    def setbit(self, index, colour, bit):
        r = self.get_row(index)
        r.setbit(colour, bit)
        self.set_row(index, r)

    def getbit(self, index, colour):
        return self.get_row(index).getbit(colour)
