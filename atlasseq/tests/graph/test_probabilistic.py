"""Tests that are unique to Probabilistic Graphs"""
from atlasseq import ProbabilisticMultiColourDeBruijnGraph as Graph
import random
from atlasseq.utils import make_hash
from atlasseq.utils import reverse_comp
from hypothesis import given
from hypothesis import example
import hypothesis.strategies as st
from atlasseq.bytearray import ByteArray
import tempfile
import os

REDIS_HOST = os.environ.get("REDIS_IP_1", 'localhost')
REDIS_PORT = os.environ.get("REDIS_PORT_1", '6379')
POSSIBLE_STORAGES = [
    # {'dict': None},
    # {"redis": {"conn": [(REDIS_HOST, REDIS_PORT, 2)]}},
    {"redis-cluster": {"conn": [(REDIS_HOST, REDIS_PORT, 2)]}},
    # {'berkeleydb': {'filename': './db'}},
    # {'leveldb': {'filename': './db2'}}
]
st_storage = st.sampled_from(POSSIBLE_STORAGES)
st_sample_colour = st.integers(min_value=0, max_value=10)
st_sample_name = st.text(min_size=1)

BINARY_KMERS_OR_NOT = [True, False]
st_binary_kmers = st.sampled_from(BINARY_KMERS_OR_NOT)
KMER = st.text(min_size=31, max_size=31, alphabet=['A', 'T', 'C', 'G'])


# Add test for insert, lookup.


@given(storage=st_storage, binary_kmers=st_binary_kmers, sample=st_sample_name, kmers=st.lists(KMER, max_size=10))
def test_get_bloomfilter(storage, binary_kmers, sample, kmers):
    mc = Graph(
        binary_kmers=binary_kmers, storage=storage, bloom_filter_size=100)
    mc.delete_all()
    mc.insert(kmers, sample)
    bf = mc.get_bloom_filter(sample)
    assert bf.length() == mc.graph.bloomfilter.size


@given(kmer=KMER, store1=st_storage, store2=st_storage,
       bloom_filter_size=st.integers(min_value=100, max_value=1000), num_hashes=st.integers(min_value=1, max_value=5))
def test_dumps_loads(kmer, store1, store2, bloom_filter_size, num_hashes):
    """test dumping and loading graphs from various backends"""
    mc = Graph(
        storage=store1, bloom_filter_size=bloom_filter_size, num_hashes=num_hashes)
    mc.delete_all()
    mc.insert(kmer, '1234')
    assert mc.lookup(kmer) == {kmer: ['1234']}
    mc.insert(kmer, '1235')
    assert mc.lookup(kmer) == {kmer: ['1234', '1235']}
    graph_dump = mc.dumps()

    mc2 = Graph(storage=store2)
    mc2.loads(graph_dump)
    assert mc2.lookup(kmer) == {kmer: ['1234', '1235']}


@given(kmer=KMER, store1=st_storage, store2=st_storage,
       bloom_filter_size=st.integers(min_value=100, max_value=1000), num_hashes=st.integers(min_value=1, max_value=5))
def test_dump_load(kmer, store1, store2, bloom_filter_size, num_hashes):
    """test dumping and loading graphs from various backends"""
    mc = Graph(
        storage=store1, bloom_filter_size=bloom_filter_size, num_hashes=num_hashes)
    mc.delete_all()
    mc.insert(kmer, '1234')
    assert mc.lookup(kmer) == {kmer: ['1234']}
    mc.insert(kmer, '1235')
    assert mc.lookup(kmer) == {kmer: ['1234', '1235']}
    mc2 = Graph(storage=store2)
    _, fp = tempfile.mkstemp()
    with open(fp, 'wb') as outfile:
        mc.dump(outfile)
    with open(fp, 'rb') as infile:
        mc2.load(infile)
    assert mc2.lookup(kmer) == {kmer: ['1234', '1235']}
    os.remove(fp)
