import sys
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


from cbg.storage.graph.probabilistic import ProbabilisticBerkeleyDBStorage


from cbg.storage import BerkeleyDBStorage
from cbg.sketch import HyperLogLogJaccardIndex
from cbg.sketch import MinHashHashSet
from cbg.utils import DEFAULT_LOGGING_LEVEL
from cbg.matrix import transpose
from cbg.scoring import Scorer
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

# TODO - parameters should be set once and then read from metadata
# bloom, search commands should only need to point to the DB and
# parameters are read from there

# TODO
# create class method to init the `cbg init` - CBG().create(m=,n,)
# test if init without creating first returns error
# cbg init fails if directory already exists
import os
import bsddb3
DEFUALT_DB_DIRECTORY = "./db-cbg/"


class CBG(object):

    def __init__(self, db=DEFUALT_DB_DIRECTORY, cachesize=1):
        self.db = db
        try:
            self.metadata = BerkeleyDBStorage(
                filename=os.path.join(self.db, "metadata"), decode='utf-8')
        except (bsddb3.db.DBNoSuchFileError, bsddb3.db.DBError) as e:
            raise ValueError(
                "Cannot find a CBG at %s. Run `cbg init` or CBG.create()" % db)
        else:
            self.sample_to_colour_lookup = BerkeleyDBStorage(
                filename=os.path.join(self.db, "sample_to_colour_lookup"), decode='utf-8')
            self.colour_to_sample_lookup = BerkeleyDBStorage(
                filename=os.path.join(self.db, "colour_to_sample_lookup"), decode='utf-8')
            self.bloom_filter_size = int(self.metadata['bloom_filter_size'])
            self.num_hashes = int(self.metadata['num_hashes'])
            self.kmer_size = int(self.metadata['kmer_size'])
            self.scorer = Scorer(self.get_num_colours())
            self.graph = ProbabilisticBerkeleyDBStorage(filename=os.path.join(self.db, "graph"),
                                                        bloom_filter_size=self.bloom_filter_size,
                                                        num_hashes=self.num_hashes)

    @classmethod
    def create(cls, db=DEFUALT_DB_DIRECTORY, k=31, m=25000000, h=3, cachesize=1, force=False):
        # Initialises a CBG
        # m: bloom_filter_size
        # h: number of hash functions
        # directory - where to store the cbg
        try:
            os.mkdir(db)
        except FileExistsError:
            if force:
                logger.info("Clearing and recreating %s" % db)
                cls(db).delete_all()
                return cls.create(db=db, k=k, m=m, h=h,
                                  cachesize=cachesize, force=False)
            raise FileExistsError(
                "A CBG already exists at %s. Run with --force or CBG.create(force=True) to recreate." % db)

        else:
            logger.info("Initialising CBG at %s" % db)
            metadata_filepath = os.path.join(db, "metadata")
            metadata = BerkeleyDBStorage(
                decode='utf-8', filename=metadata_filepath)
            metadata["bloom_filter_size"] = m
            metadata["num_hashes"] = h
            metadata["kmer_size"] = k
            metadata.sync()
            return cls(db=db, cachesize=cachesize)

    def build(self, bloomfilters, samples):
        bloom_filter_size = len(bloomfilters[0])
        assert len(bloomfilters) == len(samples)
        [self._add_sample(s) for s in samples]
        cbg = transpose(bloomfilters)
        for i, ba in enumerate(cbg):
            if (i % (self.bloom_filter_size/10)) == 0:
                logger.info("%i of %i" % (i, self.bloom_filter_size))
            self.graph[i] = ba.tobytes()
        self.sync()

    @convert_kmers_to_canonical
    def bloom(self, kmers):
        logger.info("Building bloom filter")
        return self.graph.bloomfilter.create(kmers)

    def insert(self, bloom_filter, sample):
        """
           Insert kmers into the multicoloured graph.
           sample can not already exist in the graph
        """
        colour = self._add_sample(sample)
        logger.info("Inserting sample %s into colour %i" % (sample, colour))
        self._insert(bloom_filter, colour)

    def search(self, seq, threshold=1, score=True):
        return self._search(seq_to_kmers(seq, self.kmer_size), threshold=threshold, score=score)

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

    def lookup_raw(self, kmer):
        return self._lookup_raw(kmer)

    @convert_kmers_to_canonical
    def _lookup_raw(self, kmer, canonical=False):
        return self.graph.lookup(kmer).tobytes()

    def get_bloom_filter(self, sample):
        colour = self.get_colour_from_sample(sample)
        return self.graph.get_bloom_filter(colour)

    def create_bloom_filter(self, kmers):
        return self.graph.create_bloom_filter(kmers)

    def _insert(self, bloomfilter, colour):
        if bloomfilter:
            logger.debug("Inserting bloomfilter into colour %i" % colour)
            self.graph.insert(bloomfilter, int(colour))

    def colours(self, kmer):
        return {kmer: self._colours(kmer)}

    @convert_kmers_to_canonical
    def _colours(self, kmer, canonical=False):
        colour_presence_boolean_array = self.graph.lookup(kmer)
        return colour_presence_boolean_array.colours()

    def _get_kmers_colours(self, kmers):
        for kmer in kmers:
            ba = self.graph.lookup(kmer)
            yield kmer, ba

    def _search(self, kmers, threshold=1, score=True):
        """Return sample names where this kmer is present"""
        if isinstance(kmers, str):
            return self._search_kmer(kmers)
        else:
            return self._search_kmers(kmers, threshold=threshold, score=score)

    @convert_kmers_to_canonical
    def _search_kmer(self, kmer, canonical=False):
        out = {}
        colours_to_sample_dict = self.colours_to_sample_dict()
        for colour in self.colours(kmer, canonical=True):
            sample = colours_to_sample_dict.get(colour, 'missing')
            if sample != "DELETED":
                out[sample] = 1.0
        return out

    @convert_kmers_to_canonical
    def _search_kmers(self, kmers, threshold=1, score=True):
        if threshold == 1:
            return self._search_kmers_threshold_1(kmers, score=score)
        else:
            return self._search_kmers_threshold_not_1(kmers, threshold=threshold, score=score)

    def _search_kmers_threshold_not_1(self, kmers, threshold, score):
        if score:
            return self._search_kmers_threshold_not_1_with_scoring(kmers, threshold)
        else:
            return self._search_kmers_threshold_not_1_without_scoring(kmers, threshold)

    def _search_kmers_threshold_not_1_with_scoring(self, kmers, threshold):
        out = {}
        kmers = list(kmers)
        result = self._search_kmers_threshold_not_1_without_scoring(
            kmers, threshold)
        kmer_lookups = [self.graph.lookup(kmer) for kmer in kmers]
        for sample, r in result.items():
            percent = r["percent_kmers_found"]
            colour = int(self.sample_to_colour_lookup.get(sample))
            s = "".join([str(int(kmer_lookups[i][colour]))
                         for i in range(len(kmers))])
            out[sample] = self.scorer.score(s)
            out[sample]["percent_kmers_found"] = percent
        return out

    def _search_kmers_threshold_not_1_without_scoring(self, kmers, threshold):
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
                    out[sample] = {}
                    out[sample]["percent_kmers_found"] = 100*res
        return out

    def _search_kmers_threshold_1(self, kmers, score=True):
        """Special case where the threshold is 1 (can accelerate queries with AND)"""
        kmers = list(kmers)
        ba = self.graph.lookup_all_present(
            kmers)
        out = {}
        for c in ba.colours():
            sample = self.get_sample_from_colour(c)
            if sample != "DELETED":
                out[sample] = self.scorer.score(
                    "1"*(len(kmers)+self.kmer_size-1))  # Fix!
                out[sample]["percent_kmers_found"] = 100
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

    def delete_sample(self, sample_name):
        try:
            colour = int(self.get_colour_from_sample(sample_name))
        except:
            raise ValueError("Can't find sample %s" % sample_name)
        else:
            self.colour_to_sample_lookup[colour] = "DELETED"
            del self.sample_to_colour_lookup[sample_name]

    def _add_sample(self, sample_name):
        logger.debug("Adding sample %s" % sample_name)
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

    # def close(self):
    #     if isinstance(self.graph, ProbabilisticBerkeleyDBStorage):
    #         self.sample_to_colour_lookup.storage.close()
    #         self.colour_to_sample_lookup.storage.close()
    #         self.graph.storage.close()
    #         self.metadata.storage.close()

    def delete_all(self):
        self.sample_to_colour_lookup.delete_all()
        self.colour_to_sample_lookup.delete_all()
        self.graph.delete_all()
        self.metadata.delete_all()
        os.rmdir(self.db)
