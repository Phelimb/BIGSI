"""Tests that are common to graphs"""
from atlasseq import ProbabilisticMultiColourDeBruijnGraph
import random
from atlasseq.utils import make_hash
from atlasseq.utils import reverse_comp
from hypothesis import given
from hypothesis import example
import hypothesis.strategies as st
from atlasseq.bytearray import ByteArray


POSSIBLE_STORAGES = [{'dict': None},
                     {"redis": {"conn": [('localhost', 6379, 2)]}},
                     {'berkeleydb': {'filename': './db'}}]
st_storage = st.sampled_from(POSSIBLE_STORAGES)
st_sample_colour = st.integers(min_value=0, max_value=10)
st_sample_name = st.text(min_size=1)

BINARY_KMERS_OR_NOT = [True, False]
st_binary_kmers = st.sampled_from(BINARY_KMERS_OR_NOT)
KMER = st.text(min_size=31, max_size=31, alphabet=['A', 'T', 'C', 'G'])
ST_GRAPH = st.sampled_from([ProbabilisticMultiColourDeBruijnGraph])


@given(Graph=ST_GRAPH, binary_kmers=st_binary_kmers)
def test_init(Graph, binary_kmers):
    mc = Graph(binary_kmers=binary_kmers)
    assert mc.binary_kmers == binary_kmers
    assert mc.num_hashes > 0


@given(Graph=ST_GRAPH, store=st_storage, sample=st_sample_name)
def test_add_sample_metadata(Graph, store, sample):
    mc = Graph(storage=store)
    mc.delete_all()
    colour = mc.add_sample(sample)
    assert mc.get_sample_colour(sample) == colour
    assert mc.colours_to_sample_dict().get(colour) == sample


@given(Graph=ST_GRAPH, store=st_storage, sample=st_sample_name,
       kmers=st.lists(KMER, min_size=1, max_size=100), binary_kmers=st_binary_kmers)
def test_insert_lookup_kmers(Graph, store, sample, kmers, binary_kmers):
    mc = Graph(binary_kmers=binary_kmers, storage=store)
    mc.delete_all()
    mc.insert(kmers, sample)
    for kmer in kmers:
        assert sample in mc.lookup(kmer)[kmer]
        assert sample not in mc.lookup(kmer+"T")[kmer+"T"]
    assert [sample] in mc.lookup(kmers).values()


# @given(Graph=ST_GRAPH, store=st_storage, kmer=KMER, binary_kmers=st_binary_kmers)
# def test_insert_get_kmer(Graph, store, kmer, binary_kmers):
#     mc = Graph(binary_kmers=binary_kmers, storage=store)
#     mc.delete_all()
#     mc.insert_kmer(kmer, 1)
#     assert [v for v in mc.get_kmer_colours(kmer).values()] == [[1]]
#     mc.insert_kmer(kmer, 2)
#     assert [v for v in mc.get_kmer_colours(kmer).values()] == [[1, 2]]


# @given(Graph=ST_GRAPH, store=st_storage, kmers=st.lists(KMER, max_size=100),
#        binary_kmers=st_binary_kmers)
# def test_insert_get_kmers(Graph, store, kmers, binary_kmers):
#     mc = Graph(binary_kmers=binary_kmers, storage=store)
#     mc.delete_all()
#     mc.insert_kmers(kmers, 1)
#     assert [v for v in mc.get_kmers_colours(kmers).values()] == [
#         [1]]*len(kmers)
#     mc.insert_kmers(kmers, 2)
#     assert [v for v in mc.get_kmers_colours(kmers).values()] == [
#         [1, 2]]*len(kmers)


# @given(Graph=ST_GRAPH, kmer=KMER, store=st_storage, binary_kmers=st_binary_kmers)
# def test_query_kmer(Graph, kmer, store, binary_kmers):
#     mc = Graph(binary_kmers=binary_kmers, storage=store)
#     mc.delete_all()
#     mc.add_sample('1234')
#     mc.insert_kmer(kmer, 0)
#     mc.query_kmer(kmer) == {'1234': 1}
#     mc.add_sample('1235')
#     mc.insert_kmer(kmer, 1)
#     mc.query_kmer(kmer) == {'1235': 1}


# @given(Graph=ST_GRAPH, x=st.lists(KMER, min_size=5, max_size=5, unique=True),
#        store=st_storage, binary_kmers=st_binary_kmers)
# def test_query_kmers(Graph, x, store, binary_kmers):
#     # print("new test ====== ")
#     k1, k2, k3, k4, k5 = x
#     mc = Graph(binary_kmers=binary_kmers, storage=store)
#     mc.delete_all()

#     mc.add_sample('1234')
#     mc.add_sample('1235')
#     mc.add_sample('1236')

#     mc.insert_kmers([k1, k2], 0)
#     mc.insert_kmers([k1, k3], 1)
#     mc.insert_kmers([k4, k3], 2)
#     assert mc.get_num_colours() == 3
#     mc.num_colours = mc.get_num_colours()
#     # print(mc.get_kmers_colours([k1, k2]))
#     assert mc.query_kmers([k1, k2], threshold=0.5) == {
#         '1234': 1, '1235': 0.5}
#     assert mc.query_kmers([k1, k2]) == {
#         '1234': 1}
#     assert mc.query_kmers([k1, k3], threshold=0.5) == {
#         '1234': 0.5, '1235': 1, '1236': 0.5}


# @given(Graph=ST_GRAPH, kmers=st.lists(KMER, min_size=1, max_size=100, unique=True))
# def test_kmers_to_bytes(Graph, kmers):
#     mc = Graph(binary_kmers=False)
#     for kmer in kmers:
#         assert mc._bytes_to_kmer(mc._kmer_to_bytes(kmer)) == kmer
#     # print(mc._bytes_to_kmer(b'\x1bI\xe94\x82\xb2ph'))


# @given(Graph=ST_GRAPH, kmers=st.lists(KMER, min_size=10, max_size=10, unique=True),
#        binary_kmers=st_binary_kmers, store=st_storage)
# def test_count_kmers(Graph, kmers, binary_kmers, store):
#     mc = Graph(binary_kmers=binary_kmers, storage=store)
#     mc.delete_all()
#     mc.add_sample('1234')
#     mc.insert_kmers(kmers, 0, sample='1234')
#     mc.add_to_kmers_count(kmers, sample='1234')
#     assert 7 < mc.count_kmers(sample='1234') < 12
