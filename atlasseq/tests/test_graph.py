from atlasseq import McDBG
import random
from atlasseq.utils import make_hash
from atlasseq.utils import reverse_comp
from hypothesis import given
from hypothesis import example
import hypothesis.strategies as st
from atlasseq.bytearray import ByteArray
conn_config = [('localhost', 6200), ('localhost', 6201),
               ('localhost', 6202), ('localhost', 6203)]
conn_config = [('localhost', 6379)]
# ports = [6200, 6201, 6202, 6203]
KMERS = ['A', 'T', 'C', 'G']

POSSIBLE_STORAGES = [{'dict': None}, {'berkeleydb': {'filename': './db'}},
                     {"redis": [('localhost', 6379)]},
                     {"probabilistic-inmemory":
                         {"array_size": 5000000, "num_hashes": 2}},
                     {"probabilistic-redis": {"conn": [('localhost', 6379), ('localhost', 6380)], "array_size": 100000, "num_hashes": 2}}]

storage_no_berkeley = st.sampled_from([{'dict': None}, {"redis": [
    ('localhost', 6379)]},
    {"probabilistic-inmemory":
     {"array_size": 5000000, "num_hashes": 2}},
    {"probabilistic-redis": {"conn": [('localhost', 6379), ('localhost', 6380)], "array_size": 100000, "num_hashes": 2}}])
st_storage = st.sampled_from(POSSIBLE_STORAGES)
st_sample_colour = st.integers(min_value=0, max_value=1000000)

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


@given(store=storage_no_berkeley, compress_kmers=st_compress_kmers, primary_colour=st_sample_colour, secondary_colour=st_sample_colour, diffs=st.lists(st_sample_colour, max_size=1000))
def test_insert_primary_secondary_diffs(store, compress_kmers, primary_colour, secondary_colour, diffs):
    mc = McDBG(
        conn_config=conn_config, compress_kmers=compress_kmers, storage=store)
    mc.delete_all()
    mc.insert_primary_secondary_diffs(primary_colour, secondary_colour, diffs)
    for i in diffs:
        assert secondary_colour in mc.lookup_primary_secondary_diff(
            primary_colour, i)


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
    # print(mc.get_kmers_colours([k1, k2]))
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


@given(kmers=st.lists(KMER, min_size=10, max_size=10, unique=True), compress_kmers=st_compress_kmers, store=storage_no_berkeley)
def test_count_kmers(kmers, compress_kmers, store):
    mc = McDBG(
        conn_config=conn_config, compress_kmers=compress_kmers, storage=store)
    mc.delete_all()
    mc.add_sample('1234')
    mc.insert_kmers(kmers, 0, sample='1234')
    mc.add_to_kmers_count(kmers, sample='1234')
    assert 8 < mc.count_kmers(sample='1234') < 12


# Todo - test this accross multiple backends
@given(kmers=st.lists(KMER, min_size=10, max_size=10, unique=True), compress_kmers=st_compress_kmers)
def test_jaccard_simillarity(kmers, compress_kmers):
    mc = McDBG(
        conn_config=conn_config, compress_kmers=compress_kmers, storage={"probabilistic-redis": {"conn": [('localhost', 6379), ('localhost', 6380)], "array_size": 100000, "num_hashes": 2}})
    mc.delete_all()
    mc.add_sample('1234')
    mc.add_sample('1235')
    mc.insert_kmers(kmers, 0)
    mc.insert_kmers(kmers, 1)
    mc.add_to_kmers_count(kmers, sample='1234')
    mc.add_to_kmers_count(kmers, sample='1235')
    assert mc.jaccard_simillarity('1234', '1235') == 1


@given(kmers1=st.lists(KMER, min_size=10, max_size=10, unique=True),
       kmers2=st.lists(KMER, min_size=10, max_size=10, unique=True),
       compress_kmers=st_compress_kmers)
def test_jaccard_simillarity2(kmers1, kmers2, compress_kmers):
    mc = McDBG(
        conn_config=conn_config, compress_kmers=compress_kmers, storage={"probabilistic-redis": {"conn": [('localhost', 6379), ('localhost', 6380)], "array_size": 100000, "num_hashes": 2}})
    mc.delete_all()
    mc.add_sample('1234')
    mc.add_sample('1235')
    mc.insert_kmers(kmers1, 0)
    mc.insert_kmers(kmers2, 1)
    mc.add_to_kmers_count(kmers1, sample='1234')
    mc.add_to_kmers_count(kmers2, sample='1235')
    skmers1 = set(kmers1)
    skmers2 = set(kmers2)
    true_sim = float(len(skmers1 & skmers2)) / float(len(skmers1 | skmers2))
    assert true_sim*.9 <= mc.jaccard_simillarity(
        '1234', '1235') <= true_sim*1.1


@given(kmers1=st.lists(KMER, min_size=10, max_size=10, unique=True),
       kmers2=st.lists(KMER, min_size=10, max_size=10, unique=True),
       compress_kmers=st_compress_kmers)
def test_kmer_diff(kmers1, kmers2, compress_kmers):
    mc = McDBG(
        conn_config=conn_config, compress_kmers=compress_kmers, storage={"probabilistic-redis": {"conn": [('localhost', 6379), ('localhost', 6380)], "array_size": 100000, "num_hashes": 2}})
    mc.delete_all()
    mc.add_sample('1234')
    mc.add_sample('1235')
    mc.insert_kmers(kmers1, 0)
    mc.insert_kmers(kmers2, 1)
    mc.add_to_kmers_count(kmers1, sample='1234')
    mc.add_to_kmers_count(kmers2, sample='1235')
    skmers1 = set(kmers1)
    skmers2 = set(kmers2)
    true_diff = float(len(skmers1 ^ skmers2))
    # true_diff2 = float(len(skmers2 - skmers1))
    assert true_diff*.9 <= mc.symmetric_difference(
        '1234', '1235') <= true_diff*1.1
    true_diff = float(len(skmers1 - skmers2))

    assert true_diff*.9 <= mc.difference(
        '1234', '1235') <= true_diff*1.1
    true_diff = float(len(skmers2 - skmers1))

    assert true_diff*.9 <= mc.difference(
        '1235', '1234') <= true_diff*1.1
