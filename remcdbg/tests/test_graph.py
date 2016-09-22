from remcdbg import McDBG
import random
from remcdbg.utils import make_hash
from remcdbg.utils import reverse_comp
from hypothesis import given
import hypothesis.strategies as st
from remcdbg.bytearray import ByteArray
conn_config = [('localhost', 6200), ('localhost', 6201),
               ('localhost', 6202), ('localhost', 6203)]
conn_config = [('localhost', 6379)]
#ports = [6200, 6201, 6202, 6203]
KMERS = ['A', 'T', 'C', 'G']

POSSIBLE_STORAGES = [{'dict': None}, {'berkeleydb': {'filename': './db'}},
                     {"redis": [('localhost', 6379)]},
                     {"probabilistic-inmemory":
                         {"array_size": 5000000, "num_hashes": 2}},
                     {"probabilistic-redis": {"conn": ('localhost', 6379), "array_size": 100000, "num_hashes": 2}}]
st_storage = st.sampled_from(POSSIBLE_STORAGES)
COMPRESS_KMERS_OR_NOT = [True, False]
st_compress_kmers = st.sampled_from(COMPRESS_KMERS_OR_NOT)


@given(compress_kmers=st_compress_kmers)
def test_init(compress_kmers):
    mc = McDBG(conn_config=conn_config, compress_kmers=compress_kmers)
    assert len(mc.ports) == len(conn_config)

KMER = st.text(min_size=31, max_size=31, alphabet=['A', 'T', 'C', 'G'])


@given(store=st_storage, kmer=KMER, compress_kmers=st_compress_kmers)
def test_insert_get_kmer(store, kmer, compress_kmers):
    mc = McDBG(
        conn_config=conn_config, compress_kmers=compress_kmers, storage=store)
    mc.delete_all()
    mc.insert_kmer(kmer, 1)
    assert [v for v in mc.get_kmer_colours(kmer).values()] == [[1]]
    mc.insert_kmer(kmer, 2)
    assert [v for v in mc.get_kmer_colours(kmer).values()] == [[1, 2]]


@given(store=st_storage, kmers=st.lists(KMER), compress_kmers=st_compress_kmers)
def test_insert_get_kmers(store, kmers, compress_kmers):
    mc = McDBG(
        conn_config=conn_config, compress_kmers=compress_kmers, storage=store)
    mc.delete_all()
    mc.insert_kmers(kmers, 1)
    assert [v for v in mc.get_kmers_colours(kmers).values()] == [
        [1]]*len(kmers)
    mc.insert_kmers(kmers, 2)
    assert [v for v in mc.get_kmers_colours(kmers).values()] == [
        [1, 2]]*len(kmers)


@given(kmer=KMER, store=st_storage, compress_kmers=st_compress_kmers)
def test_query_kmer(kmer, store, compress_kmers):
    mc = McDBG(
        conn_config=conn_config, compress_kmers=compress_kmers, storage=store)
    mc.delete_all()
    mc.add_sample('1234')
    mc.insert_kmer(kmer, 0)
    mc.query_kmer(kmer) == {'1234': 1}
    mc.add_sample('1235')
    mc.insert_kmer(kmer, 1)
    mc.query_kmer(kmer) == {'1235': 1}


@given(x=st.lists(KMER, min_size=5, max_size=5, unique=True), store=st_storage, compress_kmers=st_compress_kmers)
def test_query_kmers(x, store, compress_kmers):
    # print("new test ====== ")
    k1, k2, k3, k4, k5 = x
    mc = McDBG(
        conn_config=conn_config, compress_kmers=compress_kmers, storage=store)
    mc.delete_all()

    mc.add_sample('1234')
    mc.add_sample('1235')
    mc.add_sample('1236')

    mc.insert_kmers([k1, k2], 0)
    mc.insert_kmers([k1, k3], 1)
    mc.insert_kmers([k4, k3], 2)
    assert mc.get_num_colours() == 3
    mc.num_colours = mc.get_num_colours()
    assert mc.query_kmers([k1, k2], threshold=0.5) == {
        '1234': 1, '1235': 0.5}
    assert mc.query_kmers([k1, k2]) == {
        '1234': 1}
    assert mc.query_kmers([k1, k3], threshold=0.5) == {
        '1234': 0.5, '1235': 1, '1236': 0.5}


@given(kmers=st.lists(KMER, min_size=1, max_size=100, unique=True))
def test_kmers_to_bytes(kmers):
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    for kmer in kmers:
        assert mc._bytes_to_kmer(mc._kmer_to_bytes(kmer)) == kmer
    # print(mc._bytes_to_kmer(b'\x1bI\xe94\x82\xb2ph'))


# def test_samples():
#     mc = McDBG(conn_config=conn_config, compress_kmers=False)
#     mc.flushall()
#     assert mc.get_num_colours() == 0

#     mc.add_sample('1234')
#     mc.add_sample('1235')

#     assert mc.get_sample_colour('1234') == 0
#     assert mc.get_num_colours() == 2

#     assert mc.get_sample_colour('1235') == 1
#     assert mc.get_num_colours() == 2
