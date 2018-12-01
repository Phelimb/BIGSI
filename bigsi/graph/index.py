import logging

from bigsi.bloom import generate_hashes
from bigsi.bloom import BloomFilter
from bigsi.matrix import transpose
from bigsi.matrix import BitMatrix
from bigsi.utils import convert_query_kmer
from bigsi.utils import bitwise_and

BLOOMFILTER_SIZE_KEY = "ksi:bloomfilter_size"
NUM_HASH_FUNCTS_KEY = "ksi:num_hashes"
logger = logging.getLogger(__name__)


class KmerSignatureIndex:

    """
    Methods for managing kmer signature indexes
    """

    def __init__(self, storage):
        self.storage = storage
        self.bitmatrix = BitMatrix(storage)
        self.bloomfilter_size = storage.get_integer(BLOOMFILTER_SIZE_KEY)
        self.num_hashes = storage.get_integer(NUM_HASH_FUNCTS_KEY)

    @classmethod
    def create(cls, storage, bloomfilters, bloomfilter_size, num_hashes, lowmem=False):
        bloomfilters = [
            bf.bitarray if isinstance(bf, BloomFilter) else bf for bf in bloomfilters
        ]
        storage.set_integer(BLOOMFILTER_SIZE_KEY, bloomfilter_size)
        storage.set_integer(NUM_HASH_FUNCTS_KEY, num_hashes)
        logger.debug("Transpose bitarrays")
        rows = transpose(bloomfilters, lowmem=lowmem)
        logger.debug("Insert rows")
        bitmatrix = BitMatrix.create(
            storage, rows, num_rows=bloomfilter_size, num_cols=len(bloomfilters)
        )
        return cls(storage)

    def lookup(self, kmers):
        if isinstance(kmers, str):
            kmers = [kmers]
        kmer_to_hashes = self.__kmers_to_hashes(kmers)
        hashes = {h for sublist in kmer_to_hashes.values() for h in sublist}
        rows = self.__batch_get_rows(hashes)
        return self.__bitwise_and_kmers(kmer_to_hashes, rows)

    def insert_bloom(self, bloomfilter, column_index):
        self.bitmatrix.insert_column(bloomfilter, column_index)

    def merge_indexes(self, ksi):
        for i in range(self.bloomfilter_size):
            r1 = self.bitmatrix.get_row(i)
            r2 = ksi.bitmatrix.get_row(i)
            r1.extend(r2)
            self.bitmatrix.set_row(i, r1)
        self.bitmatrix.set_num_cols(self.bitmatrix.num_cols + ksi.bitmatrix.num_cols)

    def __kmers_to_hashes(self, kmers):
        d = {}
        for k in set(kmers):
            d[k] = set(
                generate_hashes(
                    convert_query_kmer(k), self.num_hashes, self.bloomfilter_size
                )
            )  ## use canonical kmer to generate lookup, but report query kmer
        return d

    def __batch_get_rows(self, row_indexes):
        return dict(zip(row_indexes, self.bitmatrix.get_rows(row_indexes)))

    def __bitwise_and_kmers(self, kmer_to_hashes, rows):
        d = {}
        for k, hashes in kmer_to_hashes.items():
            subset_rows = [rows[h] for h in hashes]
            d[k] = bitwise_and(subset_rows)
        return d
