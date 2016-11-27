from atlasseq.storage.base import BaseStorage
from atlasseq.storage.graph.base import BaseGraphStorage
from atlasseq.storage import InMemoryStorage
from atlasseq.storage import RedisHashStorage
from atlasseq.storage import RedisBitArrayStorage
from atlasseq.storage import SimpleRedisStorage
from atlasseq.storage import BerkeleyDBStorage
from atlasseq.storage import LevelDBStorage
from atlasseq.utils import hash_key
from atlasseq.bytearray import ByteArray
from atlasseq.bitvector import BitArray
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
from atlasseq.utils import DEFAULT_LOGGING_LEVEL
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
            yield self.hash(element, seed)

    def add(self, element, colour):
        for index in self.hashes(element):
            self._setbit(index, colour, 1)

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

    def lookup(self, element, array_length):
        """returns the AND of row of a BloomFilterMatrix corresponding to element"""
        if isinstance(element, list):
            return self._lookup_elements(element, array_length)
        else:
            return self._lookup_element(element, array_length)

    def _lookup_elements(self, elements, array_length):
        indexes = []
        for e in elements:
            indexes.extend([h for h in self.hashes(e)])
        rows = self._get_rows(indexes, array_length)
        bas = []
        for i in range(0, len(rows), self.num_hashes):
            bas.append(self._binary_and(rows[i:i + self.num_hashes]))
        return bas

    def _lookup_element(self, element, array_length):
        indexes = self.hashes(element)
        rows = self._get_rows(indexes, array_length)
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

    def _get_row(self, index, array_length):
        return self.storage.get_row(index, array_length=array_length)

    def _get_rows(self, indexes, array_length):
        return self.storage.get_rows(indexes, array_length=array_length)

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

    def insert(self, kmers, colour):
        """Insert kmer/s into a colour"""
        if isinstance(kmers, str):
            self.bloomfilter.add(kmers, colour)
        else:
            self.bloomfilter.update(kmers, colour)

    def lookup(self, kmer, array_length):
        return self.bloomfilter.lookup(kmer, array_length=array_length)

    def lookup_all_present(self, elements, array_length):
        if not elements:
            raise ValueError(
                "You're trying to lookup a null element is your sequence search shorter than the kmer size?")
        indexes = []
        for e in elements:
            indexes.extend([h for h in self.bloomfilter.hashes(e)])
        rows = self.get_rows(indexes, array_length)
        return self.bloomfilter._binary_and(rows)

    def get_bloom_filter(self, colour):
        return self.bloomfilter.get_column(colour)

    def create_bloom_filter(self, kmers):
        return self.bloomfilter.create(kmers)

    def get_row(self, index, array_length=None):
        b = BitArray()
        b.frombytes(self.get(index, b''))
        return self._check_array_length(b, array_length)

    def _check_array_length(self, b, array_length=None):
        if array_length is None:
            return b
        else:
            # Ensure b is at least array_length long
            if b.length() < array_length:
                b.extend([False]*(array_length-b.length()))
            assert b.length() >= array_length
            return b[:array_length]

    def get_rows(self, indexes, array_length):
        return [self.get_row(i, array_length) for i in indexes]

    def set_row(self, index, b):
        self[index] = b.tobytes()

    def items(self):
        for i in range(self.bloomfilter.size):
            yield (i, self.get(i, b''))

    def dump(self, outfile, num_colours):
        for i in range(self.bloomfilter.size):
            v = self.get_row(i, array_length=num_colours)
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


class ProbabilisticRedisHashStorage(BaseProbabilisticStorage, RedisHashStorage):

    def __init__(self, config={"conn": [('localhost', 6379)]}, bloom_filter_size=1000000, num_hashes=3):
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabilistic-redis'

    def get_rows(self, indexes, array_length):
        indexes = [i for i in indexes]
        bas = []
        rows = self._get_raw_rows(indexes, array_length)
        for r in rows:
            b = BitArray()
            if r is None:
                r = b''
            b.frombytes(r)
            bas.append(self._check_array_length(b, array_length))
        return bas

    def _get_raw_rows(self, indexes, array_length):
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

    def get_rows(self, indexes, array_length):
        indexes = list(indexes)
        bas = []
        rows = self._get_raw_rows(indexes)
        for r in rows:
            b = BitArray()
            if r is None:
                b.append(False)
            else:
                b.frombytes(r)
            bas.append(self._check_array_length(b, array_length))
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


class ProbabilisticLevelDBStorage(BaseProbabilisticStorage, LevelDBStorage):

    def __init__(self, config={'filename': './db'}, bloom_filter_size=1000000, num_hashes=3):
        super().__init__(config, bloom_filter_size, num_hashes)
        self.name = 'probabilistic-leveldb'

    def setbits(self, indexes, colour, bit):
        for index in indexes:
            self.setbit(index, colour, bit)

    def setbit(self, index, colour, bit):
        r = self.get_row(index)
        r.setbit(colour, bit)
        self.set_row(index, r)

    def getbit(self, index, colour):
        return self.get_row(index).getbit(colour)
