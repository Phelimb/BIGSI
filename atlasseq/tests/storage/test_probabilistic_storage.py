from hypothesis import given
import hypothesis.strategies as st
from atlasseq.storage.graph.probabilistic import ProbabilisticInMemoryStorage
from atlasseq.storage.graph.probabilistic import ProbabilisticRedisHashStorage
from atlasseq.storage.graph.probabilistic import ProbabilisticRedisBitArrayStorage
from atlasseq.storage.graph.probabilistic import ProbabilisticBerkeleyDBStorage
from atlasseq.storage.graph.probabilistic import ProbabilisticLevelDBStorage
import os

REDIS_HOST = os.environ.get("REDIS_IP_1", 'localhost')
REDIS_PORT = os.environ.get("REDIS_PORT_1", '6379')
st_KMER = st.text(min_size=31, max_size=31, alphabet=['A', 'T', 'C', 'G'])

REDIS_STORAGE = {"conn": [(REDIS_HOST, REDIS_PORT, 2)]}
REDIS_CLUSTER_STORAGE = {"conn": [(REDIS_HOST, REDIS_PORT, 0)]}

POSSIBLE_STORAGES = [
    # ProbabilisticInMemoryStorage(),
    # ProbabilisticRedisHashStorage(REDIS_STORAGE),
    ProbabilisticRedisBitArrayStorage(REDIS_CLUSTER_STORAGE),
    # ProbabilisticBerkeleyDBStorage({'filename': './db'}),
]

st_storage = st.sampled_from(POSSIBLE_STORAGES)
st_colour = st.integers(min_value=0, max_value=1000)


@given(storage=st_storage, colour=st_colour, element=st_KMER,
       bloom_filter_size=st.integers(10000, 1000000),
       num_hashes=st.integers(min_value=1, max_value=5))
def test_add_contains(storage, colour, element, bloom_filter_size,  num_hashes):
    storage.bloom_filter_size = bloom_filter_size
    storage.num_hashes = num_hashes
    storage.delete_all()

    storage.bloomfilter.add(element, colour)
    assert storage.bloomfilter.contains(element, colour)
    assert not storage.bloomfilter.contains(element + "a", colour)


@given(storage=st_storage, colour=st_colour, elements=st.lists(st_KMER),
       bloom_filter_size=st.integers(10000, 1000000),
       num_hashes=st.integers(min_value=1, max_value=5))
def test_update_contains(storage, colour, elements, bloom_filter_size,  num_hashes):
    storage.bloom_filter_size = bloom_filter_size
    storage.num_hashes = num_hashes
    storage.delete_all()

    storage.bloomfilter.update(elements, colour)
    for k in elements:
        assert storage.bloomfilter.contains(k, colour)


@given(storage=st_storage, colour1=st_colour, colour2=st_colour,
       element=st_KMER,
       bloom_filter_size=st.integers(10000, 1000000),
       num_hashes=st.integers(min_value=1, max_value=5))
def test_add_lookup(storage, colour1, colour2, element, bloom_filter_size,  num_hashes):
    storage.bloom_filter_size = bloom_filter_size
    storage.num_hashes = num_hashes
    storage.delete_all()
    if not colour1 == colour2:
        storage.bloomfilter.add(element, colour1)
        assert storage.bloomfilter.contains(element, colour1)
        assert not storage.bloomfilter.contains(element, colour2)
        assert storage.bloomfilter.lookup(element).getbit(colour1) == True
        assert storage.bloomfilter.lookup(element).getbit(colour2) == False

        storage.bloomfilter.add(element, colour2)
        assert storage.bloomfilter.contains(element, colour1)
        assert storage.bloomfilter.contains(element, colour2)
        assert storage.bloomfilter.lookup(element).getbit(colour1) == True
        assert storage.bloomfilter.lookup(element).getbit(colour2) == True


@given(storage=st_storage, colour1=st_colour, colour2=st_colour,
       elements=st.lists(st_KMER, min_size=1),
       bloom_filter_size=st.integers(10000, 1000000),
       num_hashes=st.integers(min_value=1, max_value=5))
def test_add_lookup_list(storage, colour1, colour2, elements, bloom_filter_size,  num_hashes):
    storage.bloom_filter_size = bloom_filter_size
    storage.num_hashes = num_hashes
    storage.delete_all()
    if not colour1 == colour2:
        storage.bloomfilter.update(elements, colour1)
        assert all([storage.bloomfilter.contains(element, colour1)
                    for element in elements])
        assert all([not storage.bloomfilter.contains(element, colour2)
                    for element in elements])
        assert all([storage.bloomfilter.lookup(elements)[i].getbit(colour1)
                    for i in range(len(elements))]) == True
        assert all([storage.bloomfilter.lookup(elements)[i].getbit(colour2) == False
                    for i in range(len(elements))])

        storage.bloomfilter.update(elements, colour2)
        assert all([storage.bloomfilter.contains(element, colour1)
                    for element in elements])
        assert all([storage.bloomfilter.contains(element, colour2)
                    for element in elements])
        assert all([storage.bloomfilter.lookup(elements)[i].getbit(
            colour1) == True for i in range(len(elements))])
        assert all([storage.bloomfilter.lookup(elements)[i].getbit(
            colour2) == True for i in range(len(elements))])
