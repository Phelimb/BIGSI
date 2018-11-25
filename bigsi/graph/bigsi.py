from bigsi.constants import DEFAULT_CONFIG
from bigsi.graph.metadata import SampleMetadata
from bigsi.graph.index import KmerSignatureIndex
from bigsi.decorators import convert_kmers_to_canonical
from bigsi.bloom import BloomFilter
from bigsi.utils import convert_query_kmers
from bigsi.storage import get_storage
import logging

logger = logging.getLogger(__name__)


def validate_build_params(bloomfilters, samples):
    if not len(bloomfilters) == len(samples):
        raise ValueError(
            "There must be the same number of bloomfilters and sample names"
        )


class BIGSI(SampleMetadata, KmerSignatureIndex):
    def __init__(self, config=None):
        if config is None:
            config = DEFAULT_CONFIG
        self.config = config
        self.storage = get_storage(config)
        SampleMetadata.__init__(self, self.storage)
        KmerSignatureIndex.__init__(self, self.storage)

    @property
    def kmer_size(self):
        return self.config["k"]

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
        sm = SampleMetadata(storage).add_samples(samples)
        ksi = KmerSignatureIndex.create(
            storage, bloomfilters, config["m"], config["h"], config.get("lowmem", False)
        )
        storage.close()  ## Need to delete LOCK files before re init
        return cls(config)

    def insert(self, bloomfilter, sample):
        logger.warning("Build and merge is preferable to insert in most cases")
        colour = self.add_sample(sample)
        self.insert_bloom(bloomfilter, colour - 1)

    def delete(self):
        self.storage.delete_all()

    # def __validate_merge(bigsi):
    #     assert self.metadata["bloom_filter_size"] == bigsi.metadata["bloom_filter_size"]
    #     assert self.metadata["num_hashes"] == bigsi.metadata["num_hashes"]
    #     assert self.metadata["kmer_size"] == bigsi.metadata["kmer_size"]

    # def merge(self, bigsi):
    #     self.__validate_merge(bigsi)
    #     self.merge_storage(bigsi)
    #     self.merge_metadata(bigsi)

    # def merge_graph(self, bigsi):
    #     for i in range(self.storage.bloom_filter_size):
    #         r = self.storage.get_row(i)
    #         r2 = bigsi.storage.get_row(i)
    #         r.extend(r2)
    #         self.storage.set_row(i, r)

    # def merge_metadata(self, bigsi):
    #     for c in range(bigsi.metadata.num_colours):
    #         sample = bigsi.colour_to_sample(c)
    #         try:
    #             self.add_sample(sample)
    #         except ValueError:
    #             self.add_sample(sample + "_duplicate_in_merge")

    # def search(self, seq, threshold=1, score=False):
    #     assert threshold <= 1
    #     self.__validate_search_query(seq)
    #     return self.__search(self.__seq_to_kmers(seq), threshold=threshold, score=score)

    # @convert_kmers_to_canonical
    # def __search(self, kmers, threshold=1, score=False):
    #     assert isinstance(kmers, list)
    #     return self.search_kmers(kmers, threshold=threshold, score=score)

    # def __search_kmers(self, kmers, threshold=1, score=False):
    #     if threshold == 1:
    #         ## Special case optimisation when T==100% (we don't need to unpack the bit arrays)
    #         return self.__search_kmers_threshold_1(kmers, score=score)
    #     else:
    #         return self.__search_kmers_threshold_not_1(
    #             kmers, threshold=threshold, score=score
    #         )

    # def __validate_search_query(self, seq):
    #     kmers = set()
    #     for k in self.__seq_to_kmers(seq):
    #         kmers.add(k)
    #         if len(kmers) > MIN_UNIQUE_KMERS_IN_QUERY:
    #             return True
    #     else:
    #         logger.warning(
    #             "Query string should contain at least %i unique kmers. Your query contained %i unique kmers, and as a result the false discovery rate may be high. In future this will become an error."
    #             % (MIN_UNIQUE_KMERS_IN_QUERY, len(kmers))
    #         )

    # def __seq_to_kmers(self, seq):
    #     return seq_to_kmers(seq, self.storage.kmer_size)

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
