"""Tests that are unique to Probabilistic Graphs"""
from cbg import ProbabilisticMultiColourDeBruijnGraph as Graph
from cbg.tests.base import ST_KMER
from cbg.tests.base import ST_SEQ
from cbg.tests.base import ST_SAMPLE_NAME
from cbg.tests.base import ST_GRAPH
from cbg.tests.base import ST_STORAGE
from cbg.tests.base import ST_BINARY_KMERS
import random
from cbg.utils import make_hash
from cbg.utils import reverse_comp
from hypothesis import given
from hypothesis import example
import hypothesis.strategies as st
from cbg.bytearray import ByteArray
import tempfile
import os
import redis
from cbg.utils import seq_to_kmers

# Add test for insert, lookup.


@given(storage=ST_STORAGE, binary_kmers=ST_STORAGE, sample=ST_SAMPLE_NAME, seq=ST_SEQ)
def test_get_bloomfilter(storage, binary_kmers, sample, seq):
    kmers = list(seq_to_kmers(seq))
    mc = Graph(
        binary_kmers=binary_kmers, storage=storage, bloom_filter_size=100)
    mc.delete_all()
    mc.insert(kmers, sample)
    bf = mc.get_bloom_filter(sample)
    assert bf.length() == mc.graph.bloomfilter.size

## TODO - fix test
# @given(kmer=ST_KMER, store1=ST_STORAGE, store2=ST_STORAGE,
#        bloom_filter_size=st.integers(min_value=1000, max_value=1000), num_hashes=st.integers(min_value=1, max_value=5))
# def test_dump_load(kmer, store1, store2, bloom_filter_size, num_hashes):
#     """test dumping and loading graphs from various backends"""
#     mc = Graph(
#         storage=store1, bloom_filter_size=bloom_filter_size, num_hashes=num_hashes)
#     mc.delete_all()
#     mc.insert(kmer, '1234')
#     assert mc.lookup(kmer) == {kmer: ['1234']}
#     mc.insert(kmer, '1235')
#     assert mc.lookup(kmer) == {kmer: ['1234', '1235']}
#     _, fp = tempfile.mkstemp()
#     mc.dump(fp)
#     mc.delete_all()
#     mc2 = Graph(
#         storage=store2, bloom_filter_size=bloom_filter_size, num_hashes=num_hashes)

#     mc2.load(fp)
#     assert mc2.lookup(kmer) == {kmer: ['1234', '1235']}
#     os.remove(fp)
#     mc2.delete_all()
