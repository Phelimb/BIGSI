import sys
import redis
import math
import uuid
import time
from collections import Counter
import json
import logging
import pickle

from atlasseq.graph.base import BaseGraph
from atlasseq.utils import seq_to_kmers

from atlasseq.utils import min_lexo
from atlasseq.utils import bits
from atlasseq.utils import kmer_to_bits
from atlasseq.utils import bits_to_kmer
from atlasseq.utils import kmer_to_bytes
from atlasseq.utils import hash_key
from atlasseq.version import __version__


from atlasseq.decorators import convert_kmers_to_canonical

from atlasseq.bytearray import ByteArray


from atlasseq.storage.graph.probabilistic import ProbabilisticInMemoryStorage
from atlasseq.storage.graph.probabilistic import ProbabilisticRedisHashStorage
from atlasseq.storage.graph.probabilistic import ProbabilisticRedisBitArrayStorage
from atlasseq.storage.graph.probabilistic import ProbabilisticBerkeleyDBStorage
from atlasseq.storage.graph.probabilistic import ProbabilisticLevelDBStorage

from atlasseq.storage import InMemoryStorage
from atlasseq.storage import SimpleRedisStorage
from atlasseq.storage import BerkeleyDBStorage
from atlasseq.storage import LevelDBStorage
from atlasseq.sketch import HyperLogLogJaccardIndex
from atlasseq.sketch import MinHashHashSet
from atlasseq.utils import DEFAULT_LOGGING_LEVEL

import logging
logging.basicConfig()

logger = logging.getLogger(__name__)
from atlasseq.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)


class ProbabilisticMultiColourDeBruijnGraph(BaseGraph):

    def __init__(self, kmer_size=31, binary_kmers=True, storage={'dict': None},
                 bloom_filter_size=20000000, num_hashes=3):
        super().__init__(kmer_size=kmer_size, binary_kmers=binary_kmers,
                         storage=storage)
        self.hll_sketch = HyperLogLogJaccardIndex()
        self.min_hash = MinHashHashSet()
        self.bloom_filter_size = self.metadata.get('bloom_filter_size')
        self.num_hashes = self.metadata.get('num_hashes')
        if self.bloom_filter_size is not None:
            self.bloom_filter_size = int(
                self.bloom_filter_size.decode('utf-8'))
            self.num_hashes = int(self.num_hashes.decode('utf-8'))
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

    def insert(self, kmers, sample, sketch_only=False):
        """
           Insert kmers into the multicoloured graph.
           sample can not already exist in the graph
        """
        colour = self._add_sample(sample)
        self._insert(kmers, colour, sketch_only=sketch_only)

    def search(self, seq, threshold=1):
        kmers = [k for k in seq_to_kmers(seq)]
        return self._search(kmers, threshold=threshold)

    def lookup(self, kmers):
        """Return sample names where these kmers is present"""
        out = {}
        if isinstance(kmers, list):
            for kmer in kmers:
                out[kmer] = self._lookup(kmer)
        else:
            out[kmers] = self._lookup(kmers)
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

    @convert_kmers_to_canonical
    def _insert(self, kmers, colour, canonical=False, sketch_only=False):
        if kmers:
            if not sketch_only:
                logger.debug("Inserting kmers")
                self.graph.insert(kmers, colour)
            self._insert_count(kmers, colour)

    def _insert_count(self, kmers, colour):
        if self.hll_sketch:
            self.hll_sketch.insert(kmers, str(colour))
        if self.min_hash:
            self.min_hash.insert(kmers, str(colour))

    @convert_kmers_to_canonical
    def _get_kmer_colours(self, kmer, canonical=False):
        colour_presence_boolean_array = self.graph.lookup(
            kmer, array_length=self.get_num_colours())
        return {kmer: colour_presence_boolean_array.colours()}

    def _get_kmers_colours(self, kmers):
        bas = self.graph.lookup(
            kmers, array_length=self.get_num_colours())
        o = {}
        for kmer, bas in zip(kmers, bas):
            o[kmer] = bas.colours()
        return o

    def _search(self, kmers, threshold=1):
        """Return sample names where this kmer is present"""
        if isinstance(kmers, list):
            return self._search_kmers(kmers, threshold=threshold)
        else:
            return self._search_kmer(kmers)

    @convert_kmers_to_canonical
    def _search_kmer(self, kmer, canonical=False):
        out = {}
        colours_to_sample_dict = self.colours_to_sample_dict()
        for colour in self._get_kmer_colours(kmer, canonical=True):
            sample = colours_to_sample_dict.get(colour, 'missing')
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
        for kmer, colours in self._get_kmers_colours(kmers).items():
            tmp.update(colours)
        out = {}
        for k, f in tmp.items():
            res = f/len(kmers)
            if res >= threshold:
                out[colours_to_sample_dict.get(k, k)] = res
        return out

    def _search_kmers_threshold_1(self, kmers):
        """Special case where the threshold is 1 (can accelerate queries with AND)"""
        ba = self.graph.lookup_all_present(
            kmers, array_length=self.get_num_colours())
        out = {}
        for c in ba.colours():
            sample = self.get_sample_from_colour(c)
            out[sample] = 1.0
        return out

    @convert_kmers_to_canonical
    def _lookup(self, kmer, canonical=False):
        assert not isinstance(kmer, list)
        num_colours = self.get_num_colours()
        colour_to_sample = self.colours_to_sample_dict()
        colour_presence_boolean_array = self.graph.lookup(
            kmer, array_length=self.get_num_colours())
        samples_present = []
        for i, present in enumerate(colour_presence_boolean_array):
            if present:
                samples_present.append(colour_to_sample.get(i, "unknown"))
            if i > num_colours:
                break
        return samples_present

    def _choose_storage(self, storage_config):
        self.metadata = SimpleRedisStorage(
            {'conn': [('127.0.0.1', 6379, 0)]})
        self.sample_to_colour_lookup = SimpleRedisStorage(key="sample_to_colour",
                                                          config={'conn': [('127.0.0.1', 6379, 1)]})
        self.colour_to_sample_lookup = SimpleRedisStorage(key="colour_to_sample",
                                                          config={'conn': [('127.0.0.1', 6379, 2)]})

        if 'dict' in storage_config:
            self.graph = ProbabilisticInMemoryStorage(storage_config['dict'])
            self.metadata = InMemoryStorage(storage_config['dict'])
        elif 'redis' in storage_config:
            self.graph = ProbabilisticRedisHashStorage(storage_config['redis'])
        elif 'redis-cluster' in storage_config:
            self.graph = ProbabilisticRedisBitArrayStorage(
                storage_config['redis-cluster'])

        elif 'berkeleydb' in storage_config:
            self.graph = ProbabilisticBerkeleyDBStorage(
                storage_config['berkeleydb'])
        elif 'leveldb' in storage_config:
            self.graph = ProbabilisticLevelDBStorage(storage_config['leveldb'])

        else:
            raise ValueError(
                "Only in-memory dictionary, berkeleydb and redis are supported.")

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

    def delete_all(self):
        self.graph.delete_all()
        self.metadata.delete_all()
        if self.min_hash:
            self.min_hash.delete_all()
        """To do, fix this. Should be implemented as a hash colours -> 
        sample names"""
        # o = {}
        # for s in self.metadata.keys():
        #     if isinstance(s, bytes):
        #         s = s.decode("utf-8")
        #     if s[0] == 's':
        #         o[int(self.metadata.get(s))] = s[1:]
        # return o
