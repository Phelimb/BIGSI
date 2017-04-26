import redis
from cbg.utils import chunks
import mmh3
from cbg.storage.graph.probabilistic import ProbabilisticRedisBitArrayStorage
import logging
logging.basicConfig()

logger = logging.getLogger(__name__)
from cbg.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)
import heapq
import random
from collections import Counter
import binascii


class BaseMinHash(object):

    def __init__(self, prefix="mh_", sketch_size=10000):
        self.sketch_size = sketch_size
        self.prefix = prefix

    def insert(self, elements, colour):
        raise NotImplementedError()

    def intersection(self, colour):
        raise NotImplementedError()

    def _storage_key(self, colour):
        return '%s%s' % (self.prefix, colour)

    def calculate_min_hashes(self, elements):
        return heapq.nsmallest(n=self.sketch_size, iterable=self._hashes(elements, 1))

    def _hashes(self, elements, seed):
        for e in elements:
            yield self._hash(e, seed)

    def _hash(self, element, seed):
        # https://github.com/chrisjmccormick/MinHash/blob/master/runMinHashExample.py
        crc = binascii.crc32(element.encode()) & 0xffffffff
        hashCode = (1 * crc + 3) % 4294967311
        return hashCode  # mmh3.hash(element, seed)


class MinHashHashSet(BaseMinHash):

    def __init__(self, host='localhost', port='6379', db=3, prefix="mh_", sketch_size=500):
        super().__init__(prefix, sketch_size)
        self.storage = redis.StrictRedis(host=host, port=port, db=int(db))

    def insert(self, elements, colour):
        logger.debug("inserting %i into minhash %s" % (len(elements), colour))
        min_hashes = list(self.calculate_min_hashes(elements))
        self.storage.sadd(self._storage_key(colour), *min_hashes)
        self._insert_colours_to_minhash_hash(colour, min_hashes)

    def jaccard_index(self, *colours):
        assert len(colours) < 3
        colours = [str(c) for c in colours]
        intersections = self.intersection(colours[0])
        out = {}
        for colour, intersection in intersections.items():
            out[colour] = intersection/self.sketch_size
        if len(colours) > 1:
            return out.get(colours[1], 0)
        else:
            return out

    def _insert_colours_to_minhash_hash(self, colour, min_hashes):
        pipe = self.storage.pipeline()
        [pipe.sadd(mh, colour) for mh in min_hashes]
        pipe.execute()

    def _get_colours_from_minhash_hashes(self, min_hashes):
        pipe = self.storage.pipeline()
        [pipe.smembers(mh) for mh in min_hashes]
        return pipe.execute()

    def intersection(self, colour):
        min_hashes = list(self.get_min_hashes(colour))
        colours = self._get_colours_from_minhash_hashes(min_hashes)
        c = Counter()
        for cs in colours:
            cs = [
                c.decode('utf-8') if isinstance(c, bytes) else c for c in cs]
            c.update(cs)
        return dict(c)

    def get_min_hashes(self, colour):
        return self.storage.smembers(self._storage_key(colour))

    def delete_all(self):
        self.storage.flushall()


class MinHashBFMatrix(BaseMinHash):

    def __init__(self, host='localhost', port='7000', db=3, prefix="mh_", sketch_size=500):
        super().__init__(prefix, sketch_size)
        self.storage = ProbabilisticRedisBitArrayStorage(
            {"conn": [(host, port, db)]}, bloom_filter_size=50000, num_hashes=5)

    def insert(self, elements, colour):
        logger.debug("inserting %i into minhash %s" % (len(elements), colour))
        min_hashes = list(self.calculate_min_hashes(elements))
        self.storage.storage.sadd(self._storage_key(colour), *min_hashes)
        self.storage.insert([str(s) for s in min_hashes], int(colour))

    def jaccard_index(self, colour, num_colours):
        intersections = self.intersection(colour, num_colours)
        out = {}
        for colour, intersection in intersections.items():
            out[colour] = intersection/self.sketch_size
        return out

    def intersection(self, colour, num_colours):
        min_hashes = list(self.get_min_hashes(colour))
        bas = self.storage.lookup(min_hashes, num_colours)
        out = {}
        for s in range(len(bas)):
            for i in range(num_colours):
                try:
                    out[i] += bas[s][i]
                except KeyError:
                    out[i] = int(bas[s][i])
        return out

    def get_min_hashes(self, colour):
        return self.storage.storage.smembers(self._storage_key(colour))

    def delete_all(self):
        self.storage.delete_all()
