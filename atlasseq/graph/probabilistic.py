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
from atlasseq.storage.graph.probabilistic import ProbabilisticRedisStorage
from atlasseq.storage.graph.probabilistic import ProbabilisticBerkeleyDBStorage
from atlasseq.storage.graph.probabilistic import ProbabilisticLevelDBStorage

from atlasseq.storage import InMemoryStorage
from atlasseq.storage import RedisStorage
from atlasseq.storage import SimpleRedisStorage
from atlasseq.storage import BerkeleyDBStorage
from atlasseq.storage import LevelDBStorage

import logging
logging.basicConfig()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ProbabilisticMultiColourDeBruijnGraph(BaseGraph):

    def __init__(self, kmer_size=31, binary_kmers=True, storage={'dict': None},
                 bloom_filter_size=20000000, num_hashes=3):

        self.bloom_filter_size = bloom_filter_size
        self.num_hashes = num_hashes
        super().__init__(kmer_size=kmer_size, binary_kmers=binary_kmers,
                         storage=storage)

    def insert(self, kmers, sample):
        """
           Insert kmers into the multicoloured graph.
           sample can not already exist in the graph
        """
        colour = self._add_sample(sample)
        self._insert(kmers, colour)

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
        colour = self.get_sample_colour(sample)
        return self.graph.get_bloom_filter(colour)

    @convert_kmers_to_canonical
    def _insert(self, kmers, colour, canonical=False):
        self.graph.insert(kmers, colour)

    @convert_kmers_to_canonical
    def _get_kmer_colours(self, kmer, canonical=False):
        colour_presence_boolean_array = self.graph.lookup(
            kmer, num_elements=self.get_num_colours())
        return {kmer: colour_presence_boolean_array.colours()}

    def _get_kmers_colours(self, kmers):
        bas = self.graph.lookup(
            kmers, num_elements=self.get_num_colours())
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

    @convert_kmers_to_canonical
    def _lookup(self, kmer, canonical=False):
        assert not isinstance(kmer, list)
        num_colours = self.get_num_colours()
        colour_to_sample = self.colours_to_sample_dict()
        colour_presence_boolean_array = self.graph.lookup(
            kmer, num_elements=self.get_num_colours())
        samples_present = []
        for i, present in enumerate(colour_presence_boolean_array):
            if present:
                samples_present.append(colour_to_sample.get(i, "unknown"))
            if i > num_colours:
                break
        return samples_present

    def dump(self, fp):
        graph_dump = self.dumps()
        pickle.dump(graph_dump, fp)

    def dumps(self):
        d = {}
        d['version'] = __version__
        d['metadata'] = self.metadata.dumps()
        d['graph'] = self.graph.dumps()
        d['bloom_filter_size'] = self.graph.bloomfilter.size
        d['num_hashes'] = self.graph.bloomfilter.num_hashes
        return d

    def load(self, fp):
        graph_dump = pickle.load(fp)
        self.loads(graph_dump)

    def loads(self, dump):
        if not dump['version'] == __version__:
            logger.warning(
                "You're loading a graph generated by atlas-seq %s. Your version is %s. This may cause compatibility issues." % (dump['version'], __version__))
        self.metadata.loads(dump['metadata'])
        self.graph.loads(dump['graph'])
        self.bloom_filter_size = dump['bloom_filter_size']
        self.num_hashes = dump['num_hashes']
        self.graph.bloomfilter.size = self.bloom_filter_size
        self.graph.bloomfilter.num_hashes = self.num_hashes

    def _choose_storage(self, storage_config):
        if 'dict' in storage_config:
            self.graph = ProbabilisticInMemoryStorage(storage_config['dict'],
                                                      bloom_filter_size=self.bloom_filter_size,
                                                      num_hashes=self.num_hashes)
            self.metadata = InMemoryStorage(storage_config['dict'])
        elif 'redis' in storage_config:
            self.graph = ProbabilisticRedisStorage(storage_config['redis'],
                                                   bloom_filter_size=self.bloom_filter_size,
                                                   num_hashes=self.num_hashes)
            self.metadata = SimpleRedisStorage(
                {'conn': [('localhost', 6379, 0)]})
        elif 'berkeleydb' in storage_config:
            self.graph = ProbabilisticBerkeleyDBStorage(storage_config['berkeleydb'],
                                                        bloom_filter_size=self.bloom_filter_size,
                                                        num_hashes=self.num_hashes)
            self.metadata = BerkeleyDBStorage(storage_config['berkeleydb'])
        elif 'leveldb' in storage_config:
            self.graph = ProbabilisticLevelDBStorage(storage_config['leveldb'],
                                                     bloom_filter_size=self.bloom_filter_size,
                                                     num_hashes=self.num_hashes)
            self.metadata = LevelDBStorage(storage_config['leveldb'])

        else:
            raise ValueError(
                "Only in-memory dictionary, berkeleydb and redis are supported.")

    def _add_sample(self, sample_name):
        existing_index = self.get_sample_colour(sample_name)
        if existing_index is not None:
            raise ValueError("%s already exists in the db" % sample_name)
        else:
            num_colours = self.get_num_colours()
            if num_colours is None:
                num_colours = 0
            else:
                num_colours = int(num_colours)
            self.metadata['s%s' % sample_name] = num_colours
            self.metadata.incr('num_colours')
            return num_colours

    def get_sample_colour(self, sample_name):
        c = self.metadata.get('s%s' % sample_name)
        if c is not None:
            return int(c)
        else:
            return c

    def get_num_colours(self):
        try:
            return int(self.metadata.get('num_colours'))
        except TypeError:
            return 0

    def colours_to_sample_dict(self):
        o = {}
        for s in self.metadata.keys():
            if isinstance(s, bytes):
                s = s.decode("utf-8")
            if s[0] == 's':
                o[int(self.metadata.get(s))] = s[1:]
        return o
