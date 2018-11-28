import logging
import math
from multiprocessing import Pool
import numpy as np
from bigsi.constants import DEFAULT_CONFIG
from bigsi.graph.metadata import SampleMetadata
from bigsi.graph.index import KmerSignatureIndex
from bigsi.decorators import convert_kmers_to_canonical
from bigsi.bloom import BloomFilter
from bigsi.utils import convert_query_kmers
from bigsi.utils import seq_to_kmers
from bigsi.utils import bitwise_and
from bigsi.utils import non_zero_bitarrary_positions
from bigsi.storage import get_storage
from bigsi.constants import DEFAULT_NPROC
from collections import OrderedDict

logger = logging.getLogger(__name__)


def validate_build_params(bloomfilters, samples):
    if not len(bloomfilters) == len(samples):
        raise ValueError(
            "There must be the same number of bloomfilters and sample names"
        )


MIN_UNIQUE_KMERS_IN_QUERY = 0

B_ONE = (1).to_bytes(1, byteorder="big")


def unpack_and_sum(bitarrays):
    c = 0
    for bitarry in bitarrays:
        if c == 0:
            cumsum = np.fromstring(bitarry.unpack(one=B_ONE), dtype="i1").astype("i4")
        else:
            l = np.fromstring(bitarry.unpack(one=B_ONE), dtype="i1").astype("i4")
            cumsum = np.add(cumsum, l)
        c += 1
    return cumsum


def chunks(l, n):
    n = max(1, n)
    return (l[i : i + n] for i in range(0, len(l), n))


def unpack_bitarrays(bitarrays, j):
    logger.debug("ncores: %i" % j)
    if j == 0:
        res = unpack_and_sum(bitarrays)
        return res
    else:
        n = math.ceil(float(len(bitarrays)) / j)
        p = Pool(j)
        res = p.map(unpack_and_sum, chunks(bitarrays, n))
        p.close()
        return np.sum(res, axis=0)


import json


class BigsiQueryResult:
    PERCENT_KMERS_FOUND_KEY = "percent_kmers_found"
    NUM_KMERS_KEY = "num_kmers"
    NUM_KMERS_FOUND_KEY = "num_kmers_found"
    SAMPLE_KEY = "sample_name"

    def __init__(self, sample_name, num_kmers_found, num_kmers):
        self.sample_name = sample_name
        self.num_kmers_found = num_kmers_found
        self.num_kmers = num_kmers
        self.percent_kmers_found = round(100 * float(num_kmers_found) / num_kmers, 2)

    def todict(self):
        return {
            self.PERCENT_KMERS_FOUND_KEY: self.percent_kmers_found,
            self.NUM_KMERS_KEY: self.num_kmers,
            self.NUM_KMERS_FOUND_KEY: self.num_kmers_found,
            self.SAMPLE_KEY: self.sample_name,
        }

    def tojson(self):
        return json.dumps(self.todict())

    def __repr__(self):
        return self.tojson()

    def __eq__(self, ob):
        return self.todict() == ob.todict()


class BIGSI(SampleMetadata, KmerSignatureIndex):
    def __init__(self, config=None):
        if config is None:
            config = DEFAULT_CONFIG
        self.config = config
        self.storage = get_storage(config)
        SampleMetadata.__init__(self, self.storage)
        KmerSignatureIndex.__init__(self, self.storage)
        self.min_unique_kmers_in_query = (
            MIN_UNIQUE_KMERS_IN_QUERY
        )  ## TODO this can be inferred and set at build time

    @property
    def kmer_size(self):
        return self.config["k"]

    @property
    def nproc(self):
        return self.config.get("nproc", DEFAULT_NPROC)

    @classmethod
    def bloom(cls, config, kmers):
        kmers = convert_query_kmers(kmers)  ## Convert to canonical kmers
        bloomfilter = BloomFilter(m=config["m"], h=config["h"])
        bloomfilter.update(kmers)
        return bloomfilter.bitarray

    @classmethod
    def build(cls, config, bloomfilters, samples):
        storage = get_storage(config)
        validate_build_params(bloomfilters, samples)
        logger.debug("Insert sample metadata")
        sm = SampleMetadata(storage).add_samples(samples)
        logger.debug("Create signature index")
        ksi = KmerSignatureIndex.create(
            storage,
            bloomfilters,
            config["m"],
            config["h"],
            config.get("low_mem_build", False),
        )
        storage.close()  ## Need to delete LOCK files before re init
        return cls(config)

    def search(self, seq, threshold=1.0, score=False):
        self.__validate_search_query(seq)
        assert threshold <= 1
        kmers = set(self.seq_to_kmers(seq))
        kmers_to_colours = self.lookup(kmers)
        min_kmers = math.ceil(len(kmers) * threshold)
        if threshold == 1.0:
            results = self.exact_filter(kmers_to_colours)
        else:
            results = self.inexact_filter(kmers_to_colours, min_kmers)
        return results

    def exact_filter(self, kmers_to_colours):
        colours_with_all_kmers = non_zero_bitarrary_positions(
            bitwise_and(kmers_to_colours.values())
        )
        samples = self.get_sample_list(colours_with_all_kmers)
        return [
            BigsiQueryResult(
                sample_name=s,
                num_kmers=len(kmers_to_colours),
                num_kmers_found=len(kmers_to_colours),
            ).todict()
            for s in samples
        ]

    def get_sample_list(self, colours):
        colours_to_samples = self.colours_to_samples(colours)
        return [colours_to_samples[i] for i in colours]

    def inexact_filter(self, kmers_to_colours, min_kmers):
        num_kmers = unpack_bitarrays(list(kmers_to_colours.values()), self.nproc)
        colours = range(self.num_samples)
        colours_to_kmers_found = dict(zip(colours, num_kmers))
        colours_to_kmers_found_above_threshold = self.__colours_above_threshold(
            colours_to_kmers_found, min_kmers
        )
        results = [
            BigsiQueryResult(
                sample_name=self.colour_to_sample(colour),
                num_kmers_found=int(num_kmers_found),
                num_kmers=len(kmers_to_colours),
            )
            for colour, num_kmers_found in colours_to_kmers_found_above_threshold.items()
        ]
        results.sort(key=lambda x: x.num_kmers_found, reverse=True)
        return [r.todict() for r in results]

    def __colours_above_threshold(self, colours_to_percent_kmers, min_kmers):
        return {k: v for k, v in colours_to_percent_kmers.items() if v > min_kmers}

    def insert(self, bloomfilter, sample):
        logger.warning("Build and merge is preferable to insert in most cases")
        colour = self.add_sample(sample)
        self.insert_bloom(bloomfilter, colour - 1)

    def delete(self):
        self.storage.delete_all()

    def __validate_merge(self, bigsi):
        assert self.bloomfilter_size == bigsi.bloomfilter_size
        assert self.num_hashes == bigsi.num_hashes
        assert self.kmer_size == bigsi.kmer_size

    def merge(self, bigsi):
        self.__validate_merge(bigsi)
        self.merge_indexes(bigsi)
        self.merge_metadata(bigsi)

    # def __search_kmers(self, kmers, threshold=1, score=False):
    #     if threshold == 1:
    #         ## Special case optimisation when T==100% (we don't need to unpack the bit arrays)
    #         return self.__search_kmers_threshold_1(kmers, score=score)
    #     else:
    #         return self.__search_kmers_threshold_not_1(
    #             kmers, threshold=threshold, score=score
    #         )

    def __validate_search_query(self, seq):
        kmers = set()
        for k in self.seq_to_kmers(seq):
            kmers.add(k)
            if len(kmers) > self.min_unique_kmers_in_query:
                return True
        else:
            logger.warning(
                "Query string should contain at least %i unique kmers. Your query contained %i unique kmers, and as a result the false discovery rate may be high. In future this will become an error."
                % (self.min_unique_kmers_in_query, len(kmers))
            )

    def seq_to_kmers(self, seq):
        return seq_to_kmers(seq, self.kmer_size)

    # def __search_kmers_threshold_not_1(self, kmers, threshold, score):
    #     # if score:
    #     #     return self.__search_kmers_threshold_not_1_with_scoring(kmers, threshold)
    #     # else:
    #     return self.__search_kmers_threshold_not_1_without_scoring(kmers, threshold)

    # def __search_kmers_threshold_not_1_without_scoring(self, kmers, threshold):
    #     out = {}
    #     col_sum = self.storage.batch_lookup_and_sum_presence(kmers)
    #     for i, f in enumerate(col_sum):
    #         res = float(f) / len(kmers)
    #         if res >= threshold:
    #             sample = self.storage.colour_to_sample(i)
    #             if sample != "DELETED":
    #                 out[sample] = {}
    #                 out[sample]["percent_kmers_found"] = 100 * res
    #     return out

    # def __search_kmers_threshold_1(self, kmers, score=False):
    #     """Special case where the threshold is 1 (can accelerate queries with AND)"""
    #     bitarray = self.storage.batch_lookup_and_bitwise_and_presence(kmers)
    #     out = {}
    #     for c in bitarray:
    #         sample = self.storage.colour_to_sample(c)
    #         if sample != "DELETED":
    #             if score:
    #                 out[sample] = self.scorer.score(
    #                     "1" * (len(kmers) + self.kmer_size - 1)
    #                 )  # Fix!
    #             else:
    #                 out[sample] = {}
    #             out[sample]["percent_kmers_found"] = 100
    #     return out
