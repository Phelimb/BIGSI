"""Tests that are unique to Probabilistic Graphs"""
from atlasseq import ProbabilisticMultiColourDeBruijnGraph as Graph
from atlasseq.tests.base import ST_KMER
from atlasseq.tests.base import ST_SAMPLE_NAME
from atlasseq.tests.base import ST_GRAPH
from atlasseq.tests.base import ST_STORAGE
from atlasseq.tests.base import ST_BINARY_KMERS
import random
from atlasseq.utils import make_hash
from atlasseq.utils import reverse_comp
from hypothesis import given
from hypothesis import example
import hypothesis.strategies as st
from atlasseq.bytearray import ByteArray
import tempfile
import os
import redis
# Add test for insert, lookup.


@given(storage=ST_STORAGE, binary_kmers=ST_STORAGE, sample=ST_SAMPLE_NAME, kmers=st.lists(ST_KMER, max_size=10))
def test_get_bloomfilter(storage, binary_kmers, sample, kmers):
    mc = Graph(
        binary_kmers=binary_kmers, storage=storage, bloom_filter_size=100)
    mc.delete_all()
    mc.insert(kmers, sample)
    bf = mc.get_bloom_filter(sample)
    assert bf.length() == mc.graph.bloomfilter.size


# @given(kmer=ST_KMER, store1=ST_STORAGE, store2=ST_STORAGE,
#        bloom_filter_size=st.integers(min_value=100, max_value=1000), num_hashes=st.integers(min_value=1, max_value=5))
# def test_dumps_loads(kmer, store1, store2, bloom_filter_size, num_hashes):
#     """test dumping and loading graphs from various backends"""
#     mc = Graph(
#         storage=store1, bloom_filter_size=bloom_filter_size, num_hashes=num_hashes)
#     mc.delete_all()
#     mc.insert(kmer, '1234')
#     assert mc.lookup(kmer) == {kmer: ['1234']}
#     mc.insert(kmer, '1235')
#     assert mc.lookup(kmer) == {kmer: ['1234', '1235']}
#     graph_dump = mc.dumps()
#     mc.delete_all()

#     mc2 = Graph(storage=store2)
#     mc2.delete_all()
#     mc2.loads(graph_dump)
#     assert mc2.lookup(kmer) == {kmer: ['1234', '1235']}


@given(kmer=ST_KMER, store1=ST_STORAGE, store2=ST_STORAGE,
       bloom_filter_size=st.integers(min_value=1000, max_value=1000), num_hashes=st.integers(min_value=1, max_value=5))
def test_dump_load(kmer, store1, store2, bloom_filter_size, num_hashes):
    """test dumping and loading graphs from various backends"""
    r = redis.StrictRedis()
    r.flushall()
    mc = Graph(
        storage=store1, bloom_filter_size=bloom_filter_size, num_hashes=num_hashes)
    mc.delete_all()
    mc.insert(kmer, '1234')
    assert mc.lookup(kmer) == {kmer: ['1234']}
    mc.insert(kmer, '1235')
    assert mc.lookup(kmer) == {kmer: ['1234', '1235']}
    _, fp = tempfile.mkstemp()
    mc.dump(fp)
    mc.delete_all()

    mc2 = Graph(storage=store2)

    mc2.load(fp)
    assert mc2.lookup(kmer) == {kmer: ['1234', '1235']}
    os.remove(fp)
