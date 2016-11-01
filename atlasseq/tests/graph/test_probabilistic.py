"""Tests that are unique to Probabilistic Graphs"""
from atlasseq import ProbabilisticMultiColourDeBruijnGraph as Graph
import random
from atlasseq.utils import make_hash
from atlasseq.utils import reverse_comp
from hypothesis import given
from hypothesis import example
import hypothesis.strategies as st
from atlasseq.bytearray import ByteArray


POSSIBLE_STORAGES = [{'dict': None},
                     {"redis": [('localhost', 6379)]}]
st_storage = st.sampled_from(POSSIBLE_STORAGES)
st_sample_colour = st.integers(min_value=0, max_value=10)
st_sample_name = st.text(min_size=1)

BINARY_KMERS_OR_NOT = [True, False]
st_binary_kmers = st.sampled_from(BINARY_KMERS_OR_NOT)
KMER = st.text(min_size=31, max_size=31, alphabet=['A', 'T', 'C', 'G'])


# Add test for insert, lookup.


@given(binary_kmers=st_binary_kmers, primary_colour=st_sample_colour, kmers=st.lists(KMER, max_size=10))
def test_get_bloomfilter(binary_kmers, primary_colour, kmers):
    mc = Graph(
        conn_config=conn_config, binary_kmers=binary_kmers, storage=probabilistic_redis_storage)
    mc.delete_all()
    mc.insert_kmers(kmers, primary_colour)
    bf = mc.get_bloom_filter(primary_colour)
    assert len(bf) == mc.storage.array_size