import sys
import psutil
import redis
import math
import uuid
import time
from collections import Counter
import json
import logging
import pickle
import numpy as np
from cbg.graph.base import BaseGraph
from cbg.utils import seq_to_kmers

from cbg.utils import min_lexo
from cbg.utils import bits
from cbg.utils import kmer_to_bits
from cbg.utils import bits_to_kmer
from cbg.utils import kmer_to_bytes
from cbg.utils import hash_key
from cbg.version import __version__


from cbg.decorators import convert_kmers_to_canonical

from cbg.bytearray import ByteArray


from cbg.storage.graph.probabilistic import ProbabilisticInMemoryStorage
from cbg.storage.graph.probabilistic import ProbabilisticRedisHashStorage
from cbg.storage.graph.probabilistic import ProbabilisticRedisBitArrayStorage
from cbg.storage.graph.probabilistic import ProbabilisticBerkeleyDBStorage

from cbg.storage import InMemoryStorage
from cbg.storage import SimpleRedisStorage
from cbg.storage import BerkeleyDBStorage
from cbg.sketch import HyperLogLogJaccardIndex
from cbg.sketch import MinHashHashSet
from cbg.utils import DEFAULT_LOGGING_LEVEL
from cbg.matrix import transpose
from bitarray import bitarray
import logging
logging.basicConfig()

logger = logging.getLogger(__name__)
from cbg.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)


def load_bloomfilter(f):
    bloomfilter = bitarray()
    with open(f, 'rb') as inf:
        bloomfilter.fromfile(inf)
    return bloomfilter


class ProbabilisticMultiColourDeBruijnGraph(BaseGraph):

    def __init__(self, kmer_size=31, binary_kmers=True, storage={'dict': None},
                 bloom_filter_size=25000000, num_hashes=3):
        super().__init__(kmer_size=kmer_size, binary_kmers=binary_kmers,
                         storage=storage)
        self.storage = storage
        # self.hll_sketch = HyperLogLogJaccardIndex()
        # self.min_hash = MinHashHashSet()
        self.bloom_filter_size = self.metadata.get('bloom_filter_size')
        self.num_hashes = self.metadata.get('num_hashes')
        if self.bloom_filter_size is not None:
            self.bloom_filter_size = int(
                self.bloom_filter_size)
            self.num_hashes = int(self.num_hashes)
            logger.debug("BF_SIZE %i " % self.bloom_filter_size)
            if self.get_num_colours() > 0 and (bloom_filter_size != self.bloom_filter_size or num_hashes != self.num_hashes):
                raise ValueError("""This pre existing graph has settings - BFSIZE=%i;NUM_HASHES=%i.
                                        You cannot insert or query data using BFSIZE=%i;NUM_HASHES=%i""" %
                                 (self.bloom_filter_size, self.num_hashes, bloom_filter_size, num_hashes))
        else:
            self.metadata['bloom_filter_size'] = bloom_filter_size
            self.metadata['num_hashes'] = num_hashes
            self.bloom_filter_size = bloom_filter_size
            self.num_hashes = num_hashes

        self.graph.set_bloom_filter_size(self.bloom_filter_size)
        self.graph.set_num_hashes(self.num_hashes)

    def build(self, bloomfilters, samples):
        assert len(bloomfilters) == len(samples)
        [self._add_sample(s) for s in samples]
        cbg = transpose(bloomfilters)
        for i, ba in enumerate(cbg):
            if (i % self.bloom_filter_size/100) == 0:
                logger.debug("%i of %i" % (i, self.bloom_filter_size))
            self.graph[i] = ba.tobytes()
        self.sync()

    @convert_kmers_to_canonical
    def bloom(self, kmers):
        return self.graph.bloomfilter.create(kmers)

    def insert(self, bloom_filter, sample):
        """
           Insert kmers into the multicoloured graph.
           sample can not already exist in the graph
        """
        colour = self._add_sample(sample)
        self._insert(bloom_filter, colour)

    def search(self, seq, threshold=1):
        return self._search(seq_to_kmers(seq, self.kmer_size), threshold=threshold)

    def lookup(self, kmers):
        """Return sample names where these kmers is present"""
        if isinstance(kmers, str) and len(kmers) > self.kmer_size:
            kmers = seq_to_kmers(kmers, self.kmer_size)
        out = {}
        if isinstance(kmers, str):
            out[kmers] = self._lookup(kmers)

        else:
            for kmer in kmers:
                out[kmer] = self._lookup(kmer)

        return out

    def get_bloom_filter(self, sample):
        colour = self.get_colour_from_sample(sample)
        return self.graph.get_bloom_filter(colour)

    def create_bloom_filter(self, kmers):
        return self.graph.create_bloom_filter(kmers)

    def count_kmers(self, *samples):
        colours = [self.get_colour_from_sample(s) for s in samples]
        return self.hll_sketch.count(*colours)

    def dump(self, fp):

        dump = {}
        dump['version'] = __version__
        dump['metadata'] = self.metadata.dumps()
        dump['sample_to_colour_lookup'] = self.sample_to_colour_lookup.dumps()
        dump['colour_to_sample_lookup'] = self.colour_to_sample_lookup.dumps()
        dump['bloom_filter_size'] = self.graph.bloomfilter.size
        dump['num_hashes'] = self.graph.bloomfilter.num_hashes
        with open(fp+".meta", 'wb') as outfile:
            pickle.dump(dump, outfile)

        with open(fp+".graph", 'wb') as outfile:
            self.graph.dump(outfile, self.get_num_colours())

    def load(self, fp):
        with open(fp+".meta", 'rb') as infile:
            dump = pickle.load(infile)
        if not dump['version'] == __version__:
            logger.warning(
                "You're loading a graph generated by atlas-seq %s. Your version is %s. This may cause compatibility issues." % (dump['version'], __version__))
        self.metadata.loads(dump['metadata'])
        self.sample_to_colour_lookup.loads(dump['sample_to_colour_lookup'])
        self.colour_to_sample_lookup.loads(dump['colour_to_sample_lookup'])
        self.bloom_filter_size = dump['bloom_filter_size']
        self.num_hashes = dump['num_hashes']
        self.graph.set_bloom_filter_size(self.bloom_filter_size)
        self.graph.set_num_hashes(self.num_hashes)
        with open(fp+".graph", 'rb') as infile:
            self.graph.load(infile, self.get_num_colours())

    def _insert(self, bloomfilter, colour):
        if bloomfilter:
            logger.debug("Inserting BF")
            self.graph.insert(bloomfilter, int(colour))

    @convert_kmers_to_canonical
    def _get_kmer_colours(self, kmer, canonical=False):
        colour_presence_boolean_array = self.graph.lookup(
            kmer)
        return {kmer: colour_presence_boolean_array.colours()}

    def _get_kmers_colours(self, kmers):
        for kmer in kmers:
            ba = self.graph.lookup(kmer)
            yield kmer, ba

    def _search(self, kmers, threshold=1):
        """Return sample names where this kmer is present"""
        if isinstance(kmers, str):
            return self._search_kmer(kmers)
        else:
            return self._search_kmers(kmers, threshold=threshold)

    @convert_kmers_to_canonical
    def _search_kmer(self, kmer, canonical=False):
        out = {}
        colours_to_sample_dict = self.colours_to_sample_dict()
        for colour in self._get_kmer_colours(kmer, canonical=True):
            sample = colours_to_sample_dict.get(colour, 'missing')
            if sample != "DELETED":
                out[sample] = 1.0
        return out

    @convert_kmers_to_canonical
    def _search_kmers(self, kmers, threshold=1):
        if threshold == 1:
            return self._search_kmers_threshold_1(kmers)
        else:
            return self._search_kmers_threshold_not_1(kmers, threshold=threshold)

    def _search_kmers_threshold_not_1(self, kmers, threshold):
        colours_to_sample_dict = self.colours_to_sample_dict()
        tmp = Counter()
        lkmers = 0
        for kmer, ba in self._get_kmers_colours(kmers):

            if lkmers == 0:
                cumsum = np.array(ba, dtype='i4')
            else:
                l = np.array(ba, dtype='bool_')
                cumsum = np.add(cumsum, l)
            lkmers += 1
        out = {}

        # for i, f in tmp.items():
        for i, f in enumerate(cumsum):
            res = f/lkmers
            if res >= threshold:
                sample = colours_to_sample_dict.get(i, i)
                if sample != "DELETED":
                    out[sample] = res
        return out

    def _search_kmers_threshold_1(self, kmers):
        """Special case where the threshold is 1 (can accelerate queries with AND)"""
        ba = self.graph.lookup_all_present(
            kmers)
        out = {}
        for c in ba.colours():
            sample = self.get_sample_from_colour(c)
            if sample != "DELETED":
                out[sample] = 1.0
        return out

    @convert_kmers_to_canonical
    def _lookup(self, kmer, canonical=False):
        assert not isinstance(kmer, list)
        num_colours = self.get_num_colours()
        colour_to_sample = self.colours_to_sample_dict()
        colour_presence_boolean_array = self.graph.lookup(
            kmer)
        samples_present = []
        for i, present in enumerate(colour_presence_boolean_array):
            if present:
                samples_present.append(colour_to_sample.get(i, "unknown"))
            if i > num_colours:
                break
        return samples_present

    def _choose_storage(self, storage_config):

        if 'dict' in storage_config:
            self.sample_to_colour_lookup = SimpleRedisStorage(key="sample_to_colour",
                                                              config={'conn': [('127.0.0.1', 6379, 1)]})
            self.colour_to_sample_lookup = SimpleRedisStorage(key="colour_to_sample",
                                                              config={'conn': [('127.0.0.1', 6379, 2)]})
            self.graph = ProbabilisticInMemoryStorage(storage_config['dict'])
            self.metadata = InMemoryStorage(storage_config['dict'])
        elif 'redis' in storage_config:
            self.sample_to_colour_lookup = SimpleRedisStorage(key="sample_to_colour",
                                                              config={'conn': [('127.0.0.1', 6379, 1)]})
            self.colour_to_sample_lookup = SimpleRedisStorage(key="colour_to_sample",
                                                              config={'conn': [('127.0.0.1', 6379, 2)]})
            self.graph = ProbabilisticRedisHashStorage(storage_config['redis'])
            self.metadata = SimpleRedisStorage(
                {'conn': [('127.0.0.1', 6379, 0)]})
        elif 'redis-cluster' in storage_config:
            self.sample_to_colour_lookup = SimpleRedisStorage(key="sample_to_colour",
                                                              config={'conn': [('127.0.0.1', 6379, 1)]})
            self.colour_to_sample_lookup = SimpleRedisStorage(key="colour_to_sample",
                                                              config={'conn': [('127.0.0.1', 6379, 2)]})
            self.graph = ProbabilisticRedisBitArrayStorage(
                storage_config['redis-cluster'])
            self.metadata = SimpleRedisStorage(
                {'conn': [('127.0.0.1', 6379, 0)]})

        elif 'berkeleydb' in storage_config:
            filename = storage_config['berkeleydb']['filename']
            self.sample_to_colour_lookup = BerkeleyDBStorage(
                config={'decode': 'utf-8', 'filename': filename + 'sample_to_colour_lookup'})
            self.colour_to_sample_lookup = BerkeleyDBStorage(
                config={'decode': 'utf-8', 'filename': filename + 'colour_to_sample_lookup'})
            self.graph = ProbabilisticBerkeleyDBStorage(
                storage_config['berkeleydb'])
            self.metadata = BerkeleyDBStorage(
                config={'decode': 'utf-8', 'filename': filename + 'metadata'})
        else:
            raise ValueError(
                "Only in-memory dictionary, berkeleydb and redis are supported.")

    def delete_sample(self, sample_name):
        try:
            colour = int(self.get_colour_from_sample(sample_name))
        except:
            raise ValueError("Can't find sample %s" % sample_name)
        else:
            self.colour_to_sample_lookup[colour] = "DELETED"
            del self.sample_to_colour_lookup[sample_name]

    def _add_sample(self, sample_name):
        logger.debug("Adding %s" % sample_name)
        existing_index = self.get_colour_from_sample(sample_name)
        if existing_index is not None:
            raise ValueError("%s already exists in the db" % sample_name)
        else:
            colour = self.get_num_colours()
            if colour is None:
                colour = 0
            else:
                colour = int(colour)
            self.sample_to_colour_lookup[sample_name] = colour
            self.colour_to_sample_lookup[colour] = sample_name
            self.metadata.incr('num_colours')
            return colour

    def get_colour_from_sample(self, sample_name):
        c = self.sample_to_colour_lookup.get(sample_name)
        if c is not None:
            return int(c)
        else:
            return c

    def get_sample_from_colour(self, colour):
        return self.colour_to_sample_lookup.get(int(colour))

    def get_num_colours(self):
        try:
            return int(self.metadata.get('num_colours'))
        except TypeError:
            return 0

    def colours_to_sample_dict(self):
        return self.colour_to_sample_lookup

    def sync(self):
        if isinstance(self.graph, ProbabilisticBerkeleyDBStorage):
            self.sample_to_colour_lookup.storage.sync()
            self.colour_to_sample_lookup.storage.sync()
            self.graph.storage.sync()
            self.metadata.storage.sync()

    def delete_all(self):
        self.sample_to_colour_lookup.delete_all()
        self.colour_to_sample_lookup.delete_all()
        self.graph.delete_all()
        self.metadata.delete_all()
        # if self.min_hash:
        #     self.min_hash.delete_all()
