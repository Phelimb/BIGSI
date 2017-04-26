from cbg.tests.base import ST_KMER
from cbg.tests.base import ST_SEQ
from cbg.tests.base import ST_SAMPLE_NAME
from cbg.tests.base import ST_GRAPH
from cbg.tests.base import ST_BINARY_KMERS
from cbg.tests.base import ST_SAMPLE_COLOUR
from cbg.tests.base import REDIS_CLUSTER_STORAGE_CREDIS
from cbg.tests.base import REDIS_CLUSTER_STORAGE_REDIS
import hypothesis
from hypothesis import given
from hypothesis import settings
import hypothesis.strategies as st
from cbg.storage.graph.probabilistic import ProbabilisticInMemoryStorage
from cbg.storage.graph.probabilistic import ProbabilisticRedisHashStorage
from cbg.storage.graph.probabilistic import ProbabilisticRedisBitArrayStorage
from cbg.storage.graph.probabilistic import ProbabilisticBerkeleyDBStorage
from cbg.storage.graph.probabilistic import ProbabilisticLevelDBStorage
from cbg.utils import seq_to_kmers

import os

POSSIBLE_STORAGES = [
    # ProbabilisticInMemoryStorage(),
    # ProbabilisticRedisHashStorage(REDIS_STORAGE),
    # ProbabilisticRedisBitArrayStorage(REDIS_CLUSTER_STORAGE_CREDIS),
    # ProbabilisticRedisBitArrayStorage(REDIS_CLUSTER_STORAGE_REDIS),
    ProbabilisticBerkeleyDBStorage({'filename': './db'}),
]
ST_STORAGE = st.sampled_from(POSSIBLE_STORAGES)


@given(storage=ST_STORAGE, colour=ST_SAMPLE_COLOUR, element=ST_KMER,
       bloom_filter_size=st.integers(10000, 1000000),
       num_hashes=st.integers(min_value=1, max_value=5))
def test_add_contains(storage, colour, element, bloom_filter_size,  num_hashes):
    storage.bloom_filter_size = bloom_filter_size
    storage.num_hashes = num_hashes
    storage.delete_all()

    storage.bloomfilter.add(element, colour)
    assert storage.bloomfilter.contains(element, colour)
    assert not storage.bloomfilter.contains(element + "a", colour)


@given(storage=ST_STORAGE, colour=ST_SAMPLE_COLOUR, elements=ST_SEQ,
       bloom_filter_size=st.integers(10000, 1000000),
       num_hashes=st.integers(min_value=1, max_value=5))
def test_update_contains(storage, colour, elements, bloom_filter_size,  num_hashes):
    elements = list(seq_to_kmers(elements))
    storage.bloom_filter_size = bloom_filter_size
    storage.num_hashes = num_hashes
    storage.delete_all()

    storage.bloomfilter.update(elements, colour)
    for k in elements:
        assert storage.bloomfilter.contains(k, colour)


@given(storage=ST_STORAGE, colour1=ST_SAMPLE_COLOUR, colour2=ST_SAMPLE_COLOUR,
       element=ST_KMER,
       bloom_filter_size=st.integers(10000, 1000000),
       num_hashes=st.integers(min_value=1, max_value=5))
def test_add_lookup(storage, colour1, colour2, element, bloom_filter_size,  num_hashes):
    storage.bloom_filter_size = bloom_filter_size
    storage.num_hashes = num_hashes
    storage.delete_all()
    array_size = max([colour1, colour2])+1

    if not colour1 == colour2:
        storage.bloomfilter.add(element, colour1)
        assert storage.bloomfilter.contains(element, colour1)
        assert not storage.bloomfilter.contains(element, colour2)
        assert storage.bloomfilter.lookup(
            element, array_size).getbit(colour1) == True
        assert storage.bloomfilter.lookup(
            element, array_size).getbit(colour2) == False
        storage.bloomfilter.add(element, colour2)
        assert storage.bloomfilter.contains(element, colour1)
        assert storage.bloomfilter.contains(element, colour2)
        assert storage.bloomfilter.lookup(
            element, array_size).getbit(colour1) == True
        assert storage.bloomfilter.lookup(
            element, array_size).getbit(colour2) == True


@given(storage=ST_STORAGE,
       elements=st.text(
           min_size=31, max_size=100, alphabet=['A', 'T', 'C', 'G']),
       bloom_filter_size=st.integers(1000, 10000))
@settings(suppress_health_check=hypothesis.errors.Timeout)
def test_add_lookup_list(storage, elements, bloom_filter_size):
    num_hashes = 3
    colour1 = 0
    colour2 = 1
    elements = list(seq_to_kmers(elements))
    storage.bloom_filter_size = bloom_filter_size
    storage.num_hashes = num_hashes
    storage.delete_all()
    if not colour1 == colour2:
        storage.bloomfilter.update(elements, colour1)
        assert all([storage.bloomfilter.contains(element, colour1)
                    for element in elements])
        assert all([not storage.bloomfilter.contains(element, colour2)
                    for element in elements])
        array_size = max(colour1, colour2)+1
        assert all([storage.bloomfilter.lookup(elements, array_size)[i].getbit(colour1)
                    for i in range(len(elements))]) == True
        assert all([storage.bloomfilter.lookup(elements, array_size)[i].getbit(colour2) == False
                    for i in range(len(elements))])

        storage.bloomfilter.update(elements, colour2)
        assert all([storage.bloomfilter.contains(element, colour1)
                    for element in elements])
        assert all([storage.bloomfilter.contains(element, colour2)
                    for element in elements])
        assert all([storage.bloomfilter.lookup(elements, array_size)[i].getbit(
            colour1) == True for i in range(len(elements))])
        assert all([storage.bloomfilter.lookup(elements, array_size)[i].getbit(
            colour2) == True for i in range(len(elements))])


# @given(key=st.integers(min_value=0))
# def test_cluster_keyslot(key):
#     storage = ProbabilisticRedisBitArrayStorage(REDIS_CLUSTER_STORAGE_REDIS)
#     assert storage._get_key_slot(
#         key, 'python') == storage._get_key_slot(key, 'redis')
