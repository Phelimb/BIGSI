from remcdbg import McDBG
import random
from remcdbg.utils import make_hash
from remcdbg.utils import reverse_comp
conn_config = [('localhost', 6200), ('localhost', 6201),
               ('localhost', 6202), ('localhost', 6203)]
#ports = [6200, 6201, 6202, 6203]
KMERS = ['A', 'T', 'C', 'G']


def test_init():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    assert len(mc.ports) == 4


def test_set_kmer():
    k1 = 'ATCGTAGATATCGTAGATATCGTAGATATCG'
    k1_rev_comp = 'CGATATCTACGATATCTACGATATCTACGAT'
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    mc.set_kmer(k1, 1)
    print(mc.clusters['kmers'].getbit(
        k1, 1))
    assert mc.clusters['kmers'].getbit(
        k1, 1) == 1 or mc.clusters['kmers'].getbit(
        k1_rev_comp, 1) == 1
    assert mc.clusters['kmers'].getbit(
        k1, 1) == 0
    mc.flushall()
    assert mc.clusters['kmers'].getbit(
        k1, 1) == 0
    assert mc.clusters['kmers'].getbit(
        k1, 1) == 0


def test_set_kmers():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    k1 = 'ATCGTAGATATCGTAGATATCGTAGATATCG'
    k2 = 'ATCTACAATATCTACAATATCTACAATATCT'
    mc.set_kmers(
        [k1, k2], 1)
    _retrieve_kmers = mc.get_kmers(
        [k1, k2, reverse_comp(k1), reverse_comp(k2)])
    assert _retrieve_kmers[0] != None
    assert _retrieve_kmers[1] != None
    assert _retrieve_kmers[2] != None
    assert _retrieve_kmers[3] != None
    mc.flushall()


# def test_add_kmer():
#     mc = McDBG(conn_config=conn_config, compress_kmers=False)
#     mc.add_sample('s0')
#     mc.num_colours = mc.get_num_colours()
#     k1 = 'ATCGTAGATATCGTAGATATCGTAGATATCG'
#     k1_rev_comp = "CGATATCTACGATATCTACGATATCTACGAT"
#     mc.add_kmer(k1, colour=0)
#     assert mc.get_kmer(k1) == None
#     assert mc._get_set_connection(0).sismember(
#         '0', k1) == 1 or mc._get_set_connection(0).sismember('0', k1_rev_comp)
#     assert mc.query_kmers([k1]) == [(1,)]
#     assert mc.search_sets(k1, -1) == 0
#     assert mc.get_kmer(k1) == None
#     # Add the same kmer in another colour
#     mc.add_sample('s1')
#     mc.num_colours = mc.get_num_colours()
#     mc.add_kmer(k1, colour=1)
#     assert mc.get_kmer(k1) != None
#     assert mc._get_set_connection(1).sismember(
#         '1', k1) == 0
#     assert mc.query_kmers([k1]) == [(1, 1)]
#     # Add a third kmer
#     k2 = 'ATCGTAGATATCGTAGGGATCGTAGATATCG'
#     k2_rev_comp = "CGATATCTACGATCCCTACGATATCTACGAT"
#     mc.add_kmer(k2, colour=1)
#     assert mc.get_kmer(k2) == None
#     assert mc._get_set_connection(1).sismember(
#         '1', k2) == 1 or mc._get_set_connection(1).sismember('1', k2_rev_comp)
#     assert mc.query_kmers([k2]) == [(0, 1)]


def test_query_kmers():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    mc.flushall()

    mc.add_sample('1234')
    mc.add_sample('1235')
    mc.add_sample('1236')

    mc.set_kmers(
        ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'ATCTACAATATCTACAATATCTACAATATCT'], 0)
    mc.set_kmers(
        ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'ATTGTAGAGATTGTAGAGATTGTAGAGATTA'], 1)
    mc.set_kmers(['ATCGTAGAC', 'ATTGTAGAGATTGTAGAGATTGTAGAGATTA'], 2)
    assert mc.get_num_colours() == 3
    mc.num_colours = mc.get_num_colours()
    assert mc.query_kmers(['ATCGTAGATATCGTAGATATCGTAGATATCG', 'ATCTACAATATCTACAATATCTACAATATCT']) == [
        (1, 1, 0), (1, 0, 0)]
    mc.flushall()

    mc.add_sample('1234')
    mc.add_sample('1235')
    mc.add_sample('1236')

    mc.set_kmers(
        ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'CTTGTAGATCTTGTAGATCTTGTAGATCTTG'], 0)
    mc.set_kmers(
        ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'ATTGTAGAGATTGTAGAGATTGTAGAGATTA'], 1)
    mc.set_kmers(['ATCGTAGAC', 'ATTGTAGAGATTGTAGAGATTGTAGAGATTA'], 2)
    assert mc.query_kmers(['ATCGTAGATATCGTAGATATCGTAGATATCG', 'CTTGTAGATCTTGTAGATCTTGTAGATCTTG']) == [
        (1, 1, 0), (1, 0, 0)]
    mc.flushall()


def test_stats():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    mc.set_kmers(
        ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'ATCTACAATATCTACAATATCTACAATATCT'], 1)
    mc.count_kmers() == 1
    mc.calculate_memory() > 0
    mc.flushall()


def test_kmers_to_bytes():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    for i in range(100):
        kmer = "".join([random.choice(KMERS) for _ in range(31)])
        # print(kmer, mc._kmer_to_bytes(kmer),
        #       mc._bytes_to_kmer(mc._kmer_to_bytes(kmer)))
        assert mc._bytes_to_kmer(mc._kmer_to_bytes(kmer)) == kmer


def test_samples():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    mc.flushall()
    assert mc.get_num_colours() == 0

    mc.add_sample('1234')
    mc.add_sample('1235')

    assert mc.get_sample_colour('1234') == 0
    assert mc.get_num_colours() == 2

    assert mc.get_sample_colour('1235') == 1
    assert mc.get_num_colours() == 2
