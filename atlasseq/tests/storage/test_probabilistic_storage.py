from hypothesis import given
import hypothesis.strategies as st
from atlasseq.storage.probabilistic import ProbabilisticInMemoryStorage
from atlasseq.storage.probabilistic import ProbabilisticRedisStorage
st_KMER = st.text(min_size=31, max_size=31, alphabet=['A', 'T', 'C', 'G'])

REDIS_STORAGE = {"conn": [('localhost', 6379)]}

POSSIBLE_STORAGES = [
    ProbabilisticInMemoryStorage(), ProbabilisticRedisStorage(config=REDIS_STORAGE)]

st_storage = st.sampled_from(POSSIBLE_STORAGES)


@given(storage=st_storage, colour=st.integers(min_value=0, max_value=1000), element=st_KMER,
       num_hashes=st.integers(min_value=1, max_value=5))
def test_add_contains(storage, colour, element, num_hashes):
    print(storage, colour, element, num_hashes)
    storage.bloom_filter_size = 100000
    storage.num_hashes = num_hashes
    storage.bloomfilter.add(element, colour)
    assert storage.bloomfilter.contains(element, colour)
