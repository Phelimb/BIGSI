from bigsi.bloom import generate_hashes
from bigsi.bloom import BloomFilter
from bigsi.matrix import transpose
from bigsi.matrix import BitMatrix
from functools import reduce


class KmerSignatureIndex:

    """
    Methods for managing kmer signature indexes
    """

    def __init__(self, bitmatrix, bloomfilter_size, number_hash_functions):
        self.bitmatrix = bitmatrix
        self.bloomfilter_size = bloomfilter_size
        self.number_hash_functions = number_hash_functions

    @classmethod
    def create(
        cls,
        storage,
        bloomfilters,
        bloomfilter_size,
        number_hash_functions,
        lowmem=False,
    ):
        bloomfilters = [
            bf.bitarray if isinstance(bf, BloomFilter) else bf for bf in bloomfilters
        ]
        rows = list(transpose(bloomfilters, lowmem=lowmem))
        bitmatrix = BitMatrix.create(storage, rows)
        return cls(bitmatrix, bloomfilter_size, number_hash_functions)

    def __kmers_to_hashes(self, kmers):
        d = {}
        for k in set(kmers):
            d[k] = set(
                generate_hashes(k, self.number_hash_functions, self.bloomfilter_size)
            )
        return d

    def __batch_get_rows(self, row_indexes):
        return dict(zip(row_indexes, self.bitmatrix.get_rows(row_indexes)))

    def __bitwise_and_kmers(self, kmer_to_hashes, rows):
        d = {}
        for k, hashes in kmer_to_hashes.items():
            subset_rows = [rows[h] for h in hashes]
            d[k] = reduce(lambda x, y: x & y, subset_rows)
        return d

    def lookup(self, kmers):
        kmer_to_hashes = self.__kmers_to_hashes(kmers)
        hashes = {h for sublist in kmer_to_hashes.values() for h in sublist}
        rows = self.__batch_get_rows(hashes)
        return self.__bitwise_and_kmers(kmer_to_hashes, rows)
