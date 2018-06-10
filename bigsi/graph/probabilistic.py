import sys
import redis
import math
import uuid
import time
from collections import Counter
import json
import logging
import pickle
import shutil
import struct
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


# from bigsi.storage.graph.probabilistic import ProbabilisticBerkeleyDBStorage as IndexStorage
from bigsi.storage.graph.probabilistic import ProbabilisticRocksDBStorage as IndexStorage


from bigsi.storage import BerkeleyDBStorage as MetaDataStorage
# from bigsi.storage import RocksDBStorage as MetaDataStorage
from bigsi.sketch import HyperLogLogJaccardIndex
from bigsi.sketch import MinHashHashSet
from bigsi.utils import DEFAULT_LOGGING_LEVEL
from bigsi.matrix import transpose
from bigsi.scoring import Scorer
from bitarray import bitarray
import logging
logging.basicConfig()
import rocksdb

logger = logging.getLogger(__name__)
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

import math
from multiprocessing import Pool
bone = (1).to_bytes(1, byteorder='big')


def unpack_and_sum(bas):
    c = 0
    for ba in bas:
        if c == 0:
            cumsum = np.fromstring(ba.unpack(one=bone),
                                   dtype='i1').astype("i4")
        else:
            l = np.fromstring(ba.unpack(one=bone), dtype='i1').astype("i4")
            cumsum = np.add(cumsum, l)
        c += 1
    return cumsum


def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))


def unpack_bas(bas, j):
    logger.debug("ncores: %i" % j)
    if j == 0:
        res = unpack_and_sum(bas)
        return res
    else:
        n = math.ceil(float(len(bas))/j)
        p = Pool(j)
        res = p.map(unpack_and_sum, chunks(bas, n))
        p.close()
        return np.sum(res, axis=0)


class BIGSI(object):

    def __init__(self, db=DEFUALT_DB_DIRECTORY, cachesize=1, nproc=0, mode="c", metadata=None):
        self.mode = mode
        self.nproc = nproc
        self.db = db
        try:
            if metadata is None:
                self.metadata = self.load_metadata(mode)
            else:
                self.metadata=metadata
        except (bsddb3.db.DBNoSuchFileError, bsddb3.db.DBError) as e:
            print(e)
            if isinstance(e, bsddb3.db.DBError):
                raise OSError(
                    "You don't have permission to access this directory %s ." % self.db)
            else:
                raise OSError(
                    "Cannot find a BIGSI at %s. Run `bigsi init` or BIGSI.create()" % db)
        else:
            self.bloom_filter_size = struct.unpack("Q",self.metadata['bloom_filter_size'])[0]
            self.num_hashes = struct.unpack("Q",self.metadata['num_hashes'])[0]
            self.kmer_size = struct.unpack("Q",self.metadata['kmer_size'])[0]
            self.scorer = Scorer(self.get_num_colours())
            self.graph = IndexStorage(filename=self.graph_filename,
                                                        bloom_filter_size=self.bloom_filter_size,
                                                        num_hashes=self.num_hashes,
                                                        mode=mode)
            self.graph.sync()
            self.metadata.sync()

    def load_metadata(self, mode="c"):
        return MetaDataStorage(
            filename=os.path.join(self.db, "metadata"), mode=mode)

    @property
    def graph_filename(self):
        return os.path.join(self.db, "graph")

    @property
    def metadata_filename(self):
        return os.path.join(self.db, "metadata")

    def load_graph(self, mode="r"):
        return self.graph

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
                cls(db, mode="c").delete_all()
                return cls.create(db=db, k=k, m=m, h=h,
                                  cachesize=cachesize, force=False)
            raise FileExistsError(
                "A BIGSI already exists at %s. Run with --force or BIGSI.create(force=True) to recreate." % db)

        else:
            logger.info("Initialising BIGSI at %s" % db)
            metadata_filepath = os.path.join(db, "metadata")
            metadata = MetaDataStorage(filename=metadata_filepath, mode="c")
            metadata["bloom_filter_size"] = struct.pack("Q", int(m))
            metadata["num_hashes"] = struct.pack("Q", int(h))
            metadata["kmer_size"] = struct.pack("Q", int(k))
            metadata.sync()
            return cls(db=db, cachesize=cachesize, mode="c",metadata=metadata)
    ## Bdb
    # def build(self, bloomfilters, samples, lowmem=False):
    #     # Need to open with read and write access
    #     if not len(bloomfilters) == len(samples):
    #         raise ValueError(
    #             "There must be the same number of bloomfilters and sample names")
    #     graph = self.load_graph(mode="w")
    #     bloom_filter_size = len(bloomfilters[0])
    #     logger.debug("Adding samples")
    #     [self._add_sample(s, sync=False) for s in samples]
    #     logger.debug("transpose")
    #     bigsi = transpose(bloomfilters,lowmem=lowmem)
    #     logger.debug("insert")
    #     for i, ba in enumerate(bigsi):
    #         graph[i] = ba.tobytes()
    #     self.sync()

    def build(self, bloomfilters, samples, lowmem=False):
        # Need to open with read and write access
        if not len(bloomfilters) == len(samples):
            raise ValueError(
                "There must be the same number of bloomfilters and sample names")
        graph = self.load_graph(mode="w")
        bloom_filter_size = len(bloomfilters[0])
        logger.debug("Adding samples")
        [self._add_sample(s, sync=False) for s in samples]
        logger.debug("transpose")
        bigsi = transpose(bloomfilters,lowmem=lowmem)
        logger.debug("insert")
        batch = rocksdb.WriteBatch()
        _len=len(bloomfilters[0])
        for i, ba in enumerate(bigsi):
            if (i % int(_len/100))==0:
                logger.debug("Inserting row %i: %i%%" % (i, int(float(100*i)/_len)))            
                graph.storage.write(batch)
                batch = rocksdb.WriteBatch()
            batch.put(struct.pack("Q", i) , ba.tobytes())
        graph.storage.write(batch)
        self.sync()

    def merge(self, merged_bigsi):
        logger.info("Starting merge")
        # Check that they're the same length
        assert self.metadata["bloom_filter_size"] == merged_bigsi.metadata["bloom_filter_size"]
        assert self.metadata["num_hashes"] == merged_bigsi.metadata["num_hashes"]
        assert self.metadata["kmer_size"] == merged_bigsi.metadata["kmer_size"]
        self._merge_graph(merged_bigsi)
        self._merge_metadata(merged_bigsi)

    def _merge_graph(self, merged_bigsi):
        graph = self.load_graph(mode="w")
        # Update graph
        for i in range(self.bloom_filter_size):
            r = graph.get_row(i)[:self.get_num_colours()]
            r2 = merged_bigsi.graph.get_row(i)[:merged_bigsi.get_num_colours()]
            r.extend(r2)
            graph.set_row(i, r)
        graph.sync()

    def _merge_metadata(self, merged_bigsi):
        # Update metadata
        for c in range(merged_bigsi.get_num_colours()):
            sample = merged_bigsi.colour_to_sample(c)
            try:
                self._add_sample(sample, sync=False)
            except ValueError:
                self._add_sample(sample+"_duplicate_in_merge", sync=False)
        self.metadata.sync()

    @convert_kmers_to_canonical
    def bloom(self, kmers):
        logger.info("Building bloom filter")
        return self.load_graph().bloomfilter.create(kmers)

    def insert(self, bloom_filter, sample):
        """
           Insert kmers into the multicoloured graph.
           sample can not already exist in the graph
        """
        try:
            self.load_graph()[0]
        except:
            logger.error(
                "No existing index. Run `init` and `build` before `insert` or `search`")
            raise ValueError(
                "No existing index. Run `init` and `build` before `insert` or `search`")
        colour = self._add_sample(sample)
        logger.info("Inserting sample %s into colour %i" % (sample, colour))
        self._insert(bloom_filter, colour)
        self.sync()

    def search(self, seq, threshold=1, score=False):
        assert threshold <= 1
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

    def metadata_set(self, metadata_key, value, sync=True):
        metadata = self.metadata
        metadata[metadata_key] = pickle.dumps(value)
        if sync:
            self.sync()

    def metadata_hgetall(self, metadata_key):
        return pickle.loads(self.metadata.get(metadata_key, pickle.dumps({})))

    def metadata_hget(self, metadata_key, key):
        return self.metadata_hgetall(metadata_key).get(key)

    def add_sample_metadata(self, sample, key, value, overwrite=False, sync=True):
        metadata_key = "ss_%s" % sample
        self.metadata_hset(metadata_key, key, value,
                           overwrite=overwrite, sync=sync)

    def lookup_sample_metadata(self, sample):
        metadata_key = "ss_%s" % sample
        return self.metadata_hgetall(metadata_key)

    def metadata_hset(self, metadata_key, key, value, overwrite=False, sync=True):
        metadata_values = self.metadata_hgetall(metadata_key)
        if key in metadata_values and not overwrite:
            raise ValueError("%s is already in the metadata of %s with value %s " % (
                key, metadata_key, metadata_values[key]))
        else:
            metadata_values[key] = value
            self.metadata_set(metadata_key, metadata_values, sync=sync)

    def set_colour(self, colour, sample, overwrite=False, sync=True):
        colour = int(colour)
        metadata = self.metadata
        metadata["colour%i" % colour] = sample
        if sync:
            self.sync()

    def sample_to_colour(self, sample):
        return self.lookup_sample_metadata(sample).get('colour')

    def colour_to_sample(self, colour):
        metadata = self.metadata
        r = metadata["colour%i" % colour].decode('utf-8')
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
        graph = self.load_graph(mode="c")
        if bloomfilter:
            logger.debug("Inserting bloomfilter into colour %i" % colour)
            graph.insert(bloomfilter, int(colour))
            graph.sync()

    def colours(self, kmer):
        return {kmer: self._colours(kmer)}

    @convert_kmers_to_canonical
    def _colours(self, kmer, canonical=False):
        colour_presence_boolean_array = self.load_graph().lookup(kmer)
        return colour_presence_boolean_array.colours()

    def _get_kmers_colours(self, kmers):
        for kmer in kmers:
            ba = self.load_graph().lookup(kmer)
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
        kmer_lookups = [self.load_graph().lookup(kmer) for kmer in kmers]
        for colour, r in result.items():
            percent = r["percent_kmers_found"]
            s = "".join([str(int(kmer_lookups[i][colour]))
                         for i in range(len(kmers))])
            sample = self.colour_to_sample(colour)
            out[sample] = self.scorer.score(s)
            out[sample]["percent_kmers_found"] = percent
        return out

    def _search_kmers_threshold_not_1_without_scoring(self, kmers, threshold, convert_colours=True):
        out = {}
        bas = [ba for _, ba in self._get_kmers_colours(kmers)]
        cumsum = unpack_bas(bas, j=self.nproc)
        lkmers = len(bas)

        for i, f in enumerate(cumsum):
            res = float(f)/lkmers
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
        ba = self.load_graph().lookup_all_present(
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
        colour_presence_boolean_array = self.load_graph().lookup(
            kmer)
        samples_present = []
        for i, present in enumerate(colour_presence_boolean_array):
            if present:
                samples_present.append(
                    self.colour_to_sample(i))
            if i > num_colours:
                break
        return samples_present

    def _add_sample(self, sample_name, sync=True):
        sample_name = str(sample_name)
        metadata = self.metadata
        # logger.debug("Adding sample %s" % sample_name)
        existing_index = self.sample_to_colour(sample_name)
        if existing_index is not None:
            raise ValueError("%s already exists in the db" % sample_name)
        else:
            colour = self.get_num_colours()
            if colour is None:
                colour = 0
            else:
                colour = int(colour)
            self.add_sample_metadata(sample_name, 'colour', colour, sync=sync)
            self.set_colour(colour, sample_name, sync=sync)
            metadata.incr('num_colours')
            if sync:
                metadata.sync()
            return colour

    def get_num_colours(self):
        return struct.unpack("Q",self.metadata.get('num_colours', struct.pack("Q",0)))[0]

    def sync(self):
        self.graph.sync()
        self.metadata.sync()

    def delete_all(self):
        self.graph.delete_all()
        self.metadata.delete_all()
        shutil.rmtree(self.db)

    def close(self):
        self.graph.close()
        self.metadata.close()
