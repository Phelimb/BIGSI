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
from bigsi.graph.base import BaseGraph
from bigsi.utils import seq_to_kmers

from bigsi.utils import min_lexo
from bigsi.utils import bits
from bigsi.utils import kmer_to_bits
from bigsi.utils import bits_to_kmer
from bigsi.utils import kmer_to_bytes
from bigsi.utils import hash_key
from bigsi.version import __version__


from bigsi.decorators import convert_kmers_to_canonical

from bigsi.bytearray import ByteArray


from bigsi.storage.graph.probabilistic import ProbabilisticBerkeleyDBStorage


from bigsi.storage import BerkeleyDBStorage
from bigsi.sketch import HyperLogLogJaccardIndex
from bigsi.sketch import MinHashHashSet
from bigsi.utils import DEFAULT_LOGGING_LEVEL
from bigsi.matrix import transpose
from bigsi.scoring import Scorer
from bitarray import bitarray
import logging
logging.basicConfig()

logger = logging.getLogger(__name__)
from bigsi.utils import DEFAULT_LOGGING_LEVEL
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
# create class method to init the `bigsi init` - BIGSI().create(m=,n,)
# test if init without creating first returns error
# bigsi init fails if directory already exists
import os
import bsddb3
DEFUALT_DB_DIRECTORY = "./db-bigsi/"


class BIGSI(object):

    def __init__(self, db=DEFUALT_DB_DIRECTORY, cachesize=1):
        self.db = db
        try:
            self.metadata = BerkeleyDBStorage(
                filename=os.path.join(self.db, "metadata"))
        except (bsddb3.db.DBNoSuchFileError, bsddb3.db.DBError) as e:
            raise ValueError(
                "Cannot find a BIGSI at %s. Run `bigsi init` or BIGSI.create()" % db)
        else:
            self.bloom_filter_size = int.from_bytes(
                self.metadata['bloom_filter_size'], 'big')
            self.num_hashes = int.from_bytes(
                self.metadata['num_hashes'], 'big')
            self.kmer_size = int.from_bytes(self.metadata['kmer_size'], 'big')
            self.scorer = Scorer(self.get_num_colours())
            self.graph = ProbabilisticBerkeleyDBStorage(filename=os.path.join(self.db, "graph"),
                                                        bloom_filter_size=self.bloom_filter_size,
                                                        num_hashes=self.num_hashes)

    @classmethod
    def create(cls, db=DEFUALT_DB_DIRECTORY, k=31, m=25000000, h=3, cachesize=1, force=False):
        # Initialises a BIGSI
        # m: bloom_filter_size
        # h: number of hash functions
        # directory - where to store the bigsi
        try:
            os.mkdir(db)
        except FileExistsError:
            if force:
                logger.info("Clearing and recreating %s" % db)
                cls(db).delete_all()
                return cls.create(db=db, k=k, m=m, h=h,
                                  cachesize=cachesize, force=False)
            raise FileExistsError(
                "A BIGSI already exists at %s. Run with --force or BIGSI.create(force=True) to recreate." % db)

        else:
            logger.info("Initialising BIGSI at %s" % db)
            metadata_filepath = os.path.join(db, "metadata")
            metadata = BerkeleyDBStorage(filename=metadata_filepath)
            metadata["bloom_filter_size"] = (
                int(m)).to_bytes(4, byteorder='big')
            metadata["num_hashes"] = (int(h)).to_bytes(4, byteorder='big')
            metadata["kmer_size"] = (int(k)).to_bytes(4, byteorder='big')
            metadata.sync()
            return cls(db=db, cachesize=cachesize)

    def build(self, bloomfilters, samples):
        bloom_filter_size = len(bloomfilters[0])
        assert len(bloomfilters) == len(samples)
        [self._add_sample(s) for s in samples]
        bigsi = transpose(bloomfilters)
        for i, ba in enumerate(bigsi):
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

    def search(self, seq, threshold=1, score=False):
        return self._search(self.seq_to_kmers(seq), threshold=threshold, score=score)

    def lookup(self, kmers):
        """Return sample names where these kmers is present"""
        if isinstance(kmers, str) and len(kmers) > self.kmer_size:
            kmers = self.seq_to_kmers(kmers)
        out = {}
        if isinstance(kmers, str):
            out[kmers] = self._lookup(kmers)

        else:
            for kmer in kmers:
                out[kmer] = self._lookup(kmer)

        return out

    def lookup_raw(self, kmer):
        return self._lookup_raw(kmer)

    def seq_to_kmers(self, seq):
        return seq_to_kmers(seq, self.kmer_size)

    def metadata_set(self, metadata_key, value):
        self.metadata[metadata_key] = pickle.dumps(value)

    def metadata_hgetall(self, metadata_key):
        return pickle.loads(self.metadata.get(metadata_key, pickle.dumps({})))

    def metadata_hget(self, metadata_key, key):
        return self.metadata_hgetall(metadata_key).get(key)

    def add_sample_metadata(self, sample, key, value, overwrite=False):
        metadata_key = "ss_%s" % sample
        self.metadata_hset(metadata_key, key, value, overwrite=overwrite)

    def lookup_sample_metadata(self, sample):
        metadata_key = "ss_%s" % sample
        return self.metadata_hgetall(metadata_key)

    def metadata_hset(self, metadata_key, key, value, overwrite=False):
        metadata_values = self.metadata_hgetall(metadata_key)
        if key in metadata_values and not overwrite:
            raise ValueError("%s is already in the metadata of %s with value %s " % (
                key, metadata_key, metadata_values[key]))
        else:
            metadata_values[key] = value
            self.metadata_set(metadata_key, metadata_values)

    def set_colour(self, colour, sample, overwrite=False):
        colour = int(colour)
        self.metadata["colour%i" % colour] = sample

    def sample_to_colour(self, sample):
        return self.lookup_sample_metadata(sample).get('colour')

    def colour_to_sample(self, colour):
        r = self.metadata["colour%i" % colour].decode('utf-8')
        if r:
            return r
        else:
            return str(colour)

    def delete_sample(self, sample_name):
        try:
            colour = self.sample_to_colour(sample_name)
        except:
            raise ValueError("Can't find sample %s" % sample_name)
        else:
            self.set_colour(colour, "DELETED")
            self.delete_sample(sample_name)
            del self.metadata_hgetall[sample_name]

    @convert_kmers_to_canonical
    def _lookup_raw(self, kmer, canonical=False):
        return self.graph.lookup(kmer).tobytes()

    def get_bloom_filter(self, sample):
        colour = self.sample_to_colour(sample)
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

    def _search(self, kmers, threshold=1, score=False):
        """Return sample names where this kmer is present"""
        if isinstance(kmers, str):
            return self._search_kmer(kmers)
        else:
            return self._search_kmers(kmers, threshold=threshold, score=score)

    @convert_kmers_to_canonical
    def _search_kmer(self, kmer, canonical=False):
        out = {}
        for colour in self.colours(kmer, canonical=True):
            sample = self.colour_to_sample(colour)
            if sample != "DELETED":
                out[sample] = 1.0
        return out

    @convert_kmers_to_canonical
    def _search_kmers(self, kmers, threshold=1, score=False):
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
            kmers, threshold, convert_colours=False)
        kmer_lookups = [self.graph.lookup(kmer) for kmer in kmers]
        for colour, r in result.items():
            percent = r["percent_kmers_found"]
            s = "".join([str(int(kmer_lookups[i][colour]))
                         for i in range(len(kmers))])
            sample = self.colour_to_sample(colour)
            out[sample] = self.scorer.score(s)
            out[sample]["percent_kmers_found"] = percent
        return out

    def _search_kmers_threshold_not_1_without_scoring(self, kmers, threshold, convert_colours=True):
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
                if convert_colours:
                    sample = self.colour_to_sample(i)
                else:
                    sample = i
                if sample != "DELETED":
                    out[sample] = {}
                    out[sample]["percent_kmers_found"] = 100*res
        return out

    def _search_kmers_threshold_1(self, kmers, score=False):
        """Special case where the threshold is 1 (can accelerate queries with AND)"""
        kmers = list(kmers)
        ba = self.graph.lookup_all_present(
            kmers)
        out = {}
        for c in ba.colours():
            sample = self.colour_to_sample(c)
            if sample != "DELETED":
                if score:
                    out[sample] = self.scorer.score(
                        "1"*(len(kmers)+self.kmer_size-1))  # Fix!
                else:
                    out[sample] = {}
                out[sample]["percent_kmers_found"] = 100
        return out

    @convert_kmers_to_canonical
    def _lookup(self, kmer, canonical=False):
        assert not isinstance(kmer, list)
        num_colours = self.get_num_colours()
        colour_presence_boolean_array = self.graph.lookup(
            kmer)
        samples_present = []
        for i, present in enumerate(colour_presence_boolean_array):
            if present:
                samples_present.append(
                    self.colour_to_sample(i))
            if i > num_colours:
                break
        return samples_present

    def _add_sample(self, sample_name):
        logger.debug("Adding sample %s" % sample_name)
        existing_index = self.sample_to_colour(sample_name)
        if existing_index is not None:
            raise ValueError("%s already exists in the db" % sample_name)
        else:
            colour = self.get_num_colours()
            if colour is None:
                colour = 0
            else:
                colour = int(colour)
            self.add_sample_metadata(sample_name, 'colour', colour)
            self.set_colour(colour, sample_name)
            self.metadata.incr('num_colours')
            return colour

    def get_num_colours(self):
        try:
            return int.from_bytes(
                self.metadata.get('num_colours'), 'big')
        except TypeError:
            return 0

    def sync(self):
        if isinstance(self.graph, ProbabilisticBerkeleyDBStorage):
            self.graph.storage.sync()
            self.metadata.storage.sync()

    def delete_all(self):
        self.graph.delete_all()
        self.metadata.delete_all()
        os.rmdir(self.db)
