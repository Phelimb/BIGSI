import pickle
import shutil
import struct
import numpy as np
from bigsi.utils import seq_to_kmers

from bigsi.decorators import convert_kmers_to_canonical
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
    with open(f, "rb") as inf:
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

bone = (1).to_bytes(1, byteorder="big")
MIN_UNIQUE_KMERS_IN_QUERY = 50


def unpack_and_sum(bas):
    c = 0
    for ba in bas:
        if c == 0:
            cumsum = np.fromstring(ba.unpack(one=bone), dtype="i1").astype("i4")
        else:
            l = np.fromstring(ba.unpack(one=bone), dtype="i1").astype("i4")
            cumsum = np.add(cumsum, l)
        c += 1
    return cumsum


def chunks(l, n):
    n = max(1, n)
    return (l[i : i + n] for i in range(0, len(l), n))


def unpack_bas(bas, j):
    #    logger.debug("ncores: %i" % j)
    if j == 0:
        res = unpack_and_sum(bas)
        return res
    else:
        n = math.ceil(float(len(bas)) / j)
        p = Pool(j)
        res = p.map(unpack_and_sum, chunks(bas, n))
        p.close()
        return np.sum(res, axis=0)


from bigsi.constants import DEFAULT_CONFIG


class MetadataStorage:
    def __init__(self):
        pass


class RocksdbBigsiStorage:
    def __init__(self, config):
        pass


class RocksdbMetadataStorage:
    def __init__(self, config):
        pass


def __validate_build_params(bloomfilters, samples):
    if not len(bloomfilters) == len(samples):
        raise ValueError(
            "There must be the same number of bloomfilters and sample names"
        )


class BIGSI(object):
    def __init__(self, config=None):
        if config is None:
            config = DEFAULT_CONFIG
        backend_config = config["bitarray-backend"]
        metadata_config = config["metadata-backend"]
        self.storage_type = backend_config["type"]
        if self.storage_type == "rocksdb":
            self.storage = RocksdbBigsiStorage(backend_config)
            self.metadata = RocksdbMetadataStorage(backend_config)

    @convert_kmers_to_canonical
    def bloom(self, kmers):
        bloomfilter = BloomFilter(m=self.metadata.bloom_filter_size, h=self.num_hashes)
        bloomfilter.update(kmers)
        return bloomfilter

    def build(self, bloomfilters, samples):
        self.__validate_build_params(bloomfilters, samples)
        self.metadata.insert_samples(samples)
        bigsi = transpose(bloomfilters, lowmem=self.config.get("lowmem", False))
        self.storage.set_rows(bigsi)

    def insert(self, bloomfilter, sample):
        logger.warning("Build and merge is preferable to insert in most cases")
        colour = self.metadata.insert_sample(sample)
        self.storage.insert_column(bloom_filter, colour)

    def __validate_merge(bigsi):
        assert self.metadata["bloom_filter_size"] == bigsi.metadata["bloom_filter_size"]
        assert self.metadata["num_hashes"] == bigsi.metadata["num_hashes"]
        assert self.metadata["kmer_size"] == bigsi.metadata["kmer_size"]

    def merge(self, bigsi):
        self.__validate_merge(bigsi)
        self.merge_storage(bigsi)
        self.merge_metadata(bigsi)

    def merge_graph(self, bigsi):
        for i in range(self.metadata.bloom_filter_size):
            r = self.storage.get_row(i)[: self.metadata.num_colours]
            r2 = bigsi.storage.get_row(i)[: bigsi.metadata.num_colours]
            r.extend(r2)
            self.storage.set_row(i, r)
        self.storage.sync()

    def merge_metadata(self, bigsi):
        for c in range(bigsi.metadata.num_colours):
            sample = bigsi.colour_to_sample(c)
            try:
                self.add_sample(sample)
            except ValueError:
                self.add_sample(sample + "_duplicate_in_merge")

    def search(self, seq, threshold=1, score=False):
        assert threshold <= 1
        self.__validate_search_query(seq)
        return self.__search(self.__seq_to_kmers(seq), threshold=threshold, score=score)

    @convert_kmers_to_canonical
    def __search(self, kmers, threshold=1, score=False):
        assert isinstance(kmers, list)
        return self.search_kmers(kmers, threshold=threshold, score=score)

    def __search_kmers(self, kmers, threshold=1, score=False):
        if threshold == 1:
            ## Special case optimisation when T==100% (we don't need to unpack the bit arrays)
            return self.__search_kmers_threshold_1(kmers, score=score)
        else:
            return self.__search_kmers_threshold_not_1(
                kmers, threshold=threshold, score=score
            )

    def __validate_search_query(self, seq):
        kmers = set()
        for k in self.__seq_to_kmers(seq):
            kmers.add(k)
            if len(kmers) > MIN_UNIQUE_KMERS_IN_QUERY:
                return True
        else:
            logger.warning(
                "Query string should contain at least %i unique kmers. Your query contained %i unique kmers, and as a result the false discovery rate may be high. In future this will become an error."
                % (MIN_UNIQUE_KMERS_IN_QUERY, len(kmers))
            )

    def __seq_to_kmers(self, seq):
        return seq_to_kmers(seq, self.metadata.kmer_size)

    def __search_kmers_threshold_not_1(self, kmers, threshold, score):
        # if score:
        #     return self.__search_kmers_threshold_not_1_with_scoring(kmers, threshold)
        # else:
        return self.__search_kmers_threshold_not_1_without_scoring(kmers, threshold)

    def __search_kmers_threshold_not_1_without_scoring(self, kmers, threshold):
        out = {}
        col_sum = self.storage.batch_lookup_and_sum_presence(kmers)
        for i, f in enumerate(col_sum):
            res = float(f) / len(kmers)
            if res >= threshold:
                sample = self.metadata.colour_to_sample(i)
                if sample != "DELETED":
                    out[sample] = {}
                    out[sample]["percent_kmers_found"] = 100 * res
        return out

    def __search_kmers_threshold_1(self, kmers, score=False):
        """Special case where the threshold is 1 (can accelerate queries with AND)"""
        bitarray = self.storage.batch_lookup_and_bitwise_and_presence(kmers)
        out = {}
        for c in bitarray:
            sample = self.metadata.colour_to_sample(c)
            if sample != "DELETED":
                if score:
                    out[sample] = self.scorer.score(
                        "1" * (len(kmers) + self.kmer_size - 1)
                    )  # Fix!
                else:
                    out[sample] = {}
                out[sample]["percent_kmers_found"] = 100
        return out
