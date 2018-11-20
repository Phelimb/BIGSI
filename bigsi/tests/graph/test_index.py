from bigsi.storage import RedisStorage
from bigsi.storage import BerkeleyDBStorage
from bigsi.storage import RocksDBStorage
from bigsi.matrix import BitMatrix
from bigsi.bloom import BloomFilter
from bigsi.graph.index import KmerSignatureIndex
from bitarray import bitarray
import pytest


def get_storages():
    return [RedisStorage(), BerkeleyDBStorage(), RocksDBStorage()]


def test_lookup():
    kmers1 = ["ATC", "ATG", "ATA", "ATT"]
    kmers2 = ["ATC", "ATG", "ATA", "TTT"]
    bloomfilter_size = 25
    number_hash_functions = 3
    for storage in get_storages():
        bloomfilter1 = BloomFilter(bloomfilter_size, number_hash_functions).update(
            kmers1
        )
        bloomfilter2 = BloomFilter(bloomfilter_size, number_hash_functions).update(
            kmers2
        )
        bloomfilters = [bloomfilter1, bloomfilter2]
        ksi = KmerSignatureIndex.create(
            storage, bloomfilters, bloomfilter_size, number_hash_functions
        )

        assert ksi.lookup(["ATC"]) == {"ATC": bitarray("11")}
        assert ksi.lookup(["ATC", "ATC", "ATT"]) == {
            "ATC": bitarray("11"),
            "ATT": bitarray("10"),
        }
        assert ksi.lookup(["ATC", "ATC", "ATT", "TTT"]) == {
            "ATC": bitarray("11"),
            "ATT": bitarray("10"),
            "TTT": bitarray("01"),
        }
