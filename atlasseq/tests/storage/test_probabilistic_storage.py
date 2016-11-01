from hypothesis import given
import hypothesis.strategies as st
from atlasseq.storage.probabilistic import ProbabilisticInMemoryStorage
from atlasseq.storage.probabilistic import ProbabilisticRedisStorage
st_KMER = st.text(min_size=31, max_size=31, alphabet=['A', 'T', 'C', 'G'])

REDIS_STORAGE = {"conn": [('localhost', 6379)]}

POSSIBLE_STORAGES = [
    ProbabilisticInMemoryStorage(), ProbabilisticRedisStorage(REDIS_STORAGE)]

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
