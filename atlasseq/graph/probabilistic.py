import sys
import redis
import math
import uuid
import time
from collections import Counter
import json
import logging

from atlasseq.graph.base import BaseGraph

sys.path.append("../cortex-py")

from mccortex.cortex import encode_kmer
from mccortex.cortex import decode_kmer

from atlasseq.utils import min_lexo
from atlasseq.utils import bits
from atlasseq.utils import kmer_to_bits
from atlasseq.utils import bits_to_kmer
from atlasseq.utils import kmer_to_bytes
from atlasseq.utils import hash_key

from atlasseq.decorators import convert_kmers

from atlasseq.bytearray import ByteArray


from atlasseq.storage.probabilistic import ProbabilisticInMemoryStorage
from atlasseq.storage.probabilistic import ProbabilisticRedisStorage
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

    @convert_kmers
    def insert(self, sample, kmers):
        """Insert kmers into the multicoloured graph.
           sample can not already exist in the graph
        """
        raise NotImplementedError("")

    @convert_kmers
    def lookup(self, kmers):
        """Return sample names where this kmer is present"""
        raise NotImplementedError("")

    def dump(self, *args, **kwargs):
        self.storage.dump(*args, **kwargs)

    def dumps(self, *args, **kwargs):
        self.storage.dumps(*args, **kwargs)

    def load(self):
        pass

        def loads(self):
            pass

    def _choose_storage(self, storage_config):
        if 'dict' in storage_config:
            return ProbabilisticInMemoryStorage(storage_config['dict'],
                                                bloom_filter_size=self.bloom_filter_size,
                                                num_hashes=self.num_hashes)
        elif 'redis' in storage_config:
            return ProbabilisticRedisStorage(storage_config['redis'],
                                             bloom_filter_size=self.bloom_filter_size,
                                             num_hashes=self.num_hashes)
        else:
            raise ValueError(
                "Only in-memory dictionary and redis are supported.")

    @convert_kmers
    def insert_kmer(self, kmer, colour, sample=None, min_lexo=False):
        self.storage.insert_kmer(kmer, colour)

    @convert_kmers
    def insert_kmers(self, kmers, colour, sample=None, min_lexo=False):
        self.storage.insert_kmers(kmers, colour)

    def insert_primary_secondary_diffs(self, primary_colour, secondary_colour, diffs):
        self.storage.insert_primary_secondary_diffs(
            primary_colour, secondary_colour, diffs)

    def lookup_primary_secondary_diff(self, primary_colour, index):
        return self.storage.lookup_primary_secondary_diff(
            primary_colour, index)

    @convert_kmers
    def diffs_between_primary_and_secondary_bloom_filter(self, kmers, primary_colour, min_lexo=False):
        return self.storage.diffs_between_primary_and_secondary_bloom_filter(primary_colour, kmers)

    def get_bloom_filter(self, primary_colour):
        return self.storage.get_bloom_filter(primary_colour)

    @convert_kmers
    def add_to_kmers_count(self, kmers, sample, min_lexo=False):
        return self.storage.add_to_kmers_count(kmers, sample)

    @convert_kmers
    def get_kmer_raw(self, kmer, min_lexo=False):
        return self.storage.get_kmer(kmer)

    @convert_kmers
    def get_kmers_raw(self, kmers, min_lexo=False):
        return self.storage.get_kmers(kmers)

    @convert_kmers
    def get_kmer(self, kmer, min_lexo=False):
        raw = self.get_kmer_raw(kmer, min_lexo=True)
        return ByteArray(byte_array=raw)

    @convert_kmers
    def get_kmers(self, kmers, min_lexo=False):
        raws = self.get_kmers_raw(kmers, min_lexo=True)
        return [ByteArray(raw) for raw in raws]

    @convert_kmers
    def get_kmer_primary_colours(self, kmer, min_lexo=False):
        ba = self.get_kmer(kmer, min_lexo=True)
        return {kmer: ba.colours()}

    # @convert_kmers
    # def get_kmer_secondary_colours(self, kmer, min_lexo=False):
    #     primary_colours = self.get_kmer_primary_colours(kmer, min_lexo=True)
    #     secondary_colours = []
    #     for primary_colour in primary_colours:
    #         primary_colours.extend(self.storage.get_kmer_secondary_colours(kmer, primary_colour))
    #     return {kmer: ba.colours()}

    @convert_kmers
    def get_kmer_colours(self, kmer, min_lexo=False):
        return self.get_kmer_primary_colours(kmer, min_lexo=True)

    @convert_kmers
    def get_kmers_primary_colours(self, kmers, min_lexo=False):
        bas = self.get_kmers(kmers, min_lexo=True)
        o = {}
        for kmer, bas in zip(kmers, bas):
            o[kmer] = bas.colours()
        return o

    @convert_kmers
    def get_kmers_colours(self, kmers, min_lexo=False):
        return self.get_kmers_primary_colours(kmers, min_lexo=True)

    @convert_kmers
    def query_kmer(self, kmer, min_lexo=False):
        out = {}
        colours_to_sample_dict = self.colours_to_sample_dict()
        for colour in self.get_kmer_colours(kmer, min_lexo=True):
            sample = colours_to_sample_dict.get(colour, 'missing')
            out[sample] = 1
        return out

    @convert_kmers
    def query_kmers(self, kmers, min_lexo=False, threshold=1):
        colours_to_sample_dict = self.colours_to_sample_dict()
        tmp = Counter()
        for kmer, colours in self.get_kmers_colours(kmers, min_lexo=True).items():
            tmp.update(colours)

        out = {}
        for k, f in tmp.items():
            res = f/len(kmers)
            if res >= threshold:
                out[colours_to_sample_dict.get(k, k)] = res
        return out

    def kmer_union(self, sample1, sample2):
        return self.storage.kmer_union(sample1, sample2)

    def kmer_intersection(self, sample1, sample2):
        count1 = self.count_kmers(sample1)
        count2 = self.count_kmers(sample2)
        union = self.kmer_union(sample1, sample2)
        # http://dsinpractice.com/2015/09/07/counting-unique-items-fast-unions-and-intersections/
        intersection = count1+count2-union
        return intersection

    def jaccard_index(self, sample1, sample2):
        union = self.kmer_union(sample1, sample2)
        # http://dsinpractice.com/2015/09/07/counting-unique-items-fast-unions-and-intersections/
        intersection = self.kmer_intersection(sample1, sample2)
        return intersection/float(union)

    def jaccard_distance(self, sample1, sample2):
        union = self.kmer_union(sample1, sample2)
        # http://dsinpractice.com/2015/09/07/counting-unique-items-fast-unions-and-intersections/
        intersection = self.kmer_intersection(sample1, sample2)
        return (union-intersection)/float(union)

    def symmetric_difference(self, sample1, sample2):
        union = self.kmer_union(sample1, sample2)
        intersection = self.kmer_intersection(sample1, sample2)
        return union-intersection

    def difference(self, sample1, sample2):
        count1 = self.count_kmers(sample1)
        intersection = self.kmer_intersection(sample1, sample2)
        return count1-intersection

    def add_sample(self, sample_name):
        return self.storage.add_sample(sample_name)

    def get_sample_colour(self, sample_name):
        return self.storage.get_sample_colour(sample_name)

    def get_num_colours(self):
        return self.storage.get_num_colours()

    def colours_to_sample_dict(self):
        return self.storage.colours_to_sample_dict()

    def count_kmers(self, sample=None):
        return self.storage.count_kmers(sample)

    def count_keys(self):
        return self.storage.count_keys()

    def calculate_memory(self):
        return self.storage.getmemoryusage()

    def delete_all(self):
        self.storage.delete_all()

    def shutdown(self):
        [v.shutdown() for v in self.clusters.values()]

    def _kmer_to_bytes(self, kmer):
        if isinstance(kmer, str):
            return encode_kmer(kmer)
        else:
            return kmer

    def _bytes_to_kmer(self, _bytes):
        return decode_kmer(_bytes, kmer_size=self.kmer_size)

    def bitcount(self):
        self.storage.bitcount()
