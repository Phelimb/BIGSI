from hypothesis import given
import hypothesis.strategies as st
from atlasseq.storage.probabilistic import ProbabilisticInMemoryStorage
from atlasseq.storage.probabilistic import ProbabilisticRedisStorage
st_KMER = st.text(min_size=31, max_size=31, alphabet=['A', 'T', 'C', 'G'])

REDIS_STORAGE = {"conn": [('localhost', 6379)]}

POSSIBLE_STORAGES = [
    ProbabilisticInMemoryStorage(), ProbabilisticRedisStorage(REDIS_STORAGE)]

st_storage = st.sampled_from(POSSIBLE_STORAGES)


@given(storage=st_storage, colour=st.integers(max_value=1000), element=st_KMER,
       bloom_filter_size=st.integers(10000, 1000000),
       num_hashes=st.integers(min_value=1, max_value=5))
def test_add_contains(storage, colour, element, bloom_filter_size,  num_hashes):
    storage.bloom_filter_size = bloom_filter_size
    storage.num_hashes = num_hashes
    storage.bloomfilter.add(element, colour)
    assert storage.bloomfilter.contains(element, colour)
