from remcdbg.mcdbg import McDBG
import random
conn_config = [('localhost', 6200), ('localhost', 6201),
               ('localhost', 6202), ('localhost', 6203)]
#ports = [6200, 6201, 6202, 6203]
KMERS = ['A', 'T', 'C', 'G']


def test_init():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    assert len(mc.connections) == 3
    assert len(mc.connections['kmers']) == 4


def test_set_kmer():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    mc.set_kmer('ATCGTAGATATCGTAGATATCGTAGATATCG', 1)
    print(mc.connections['kmers']['A'].getbit(
        'ATCGTAGATATCGTAGATATCGTAGATATCG', 1))
    assert mc.connections['kmers']['A'].getbit(
        'ATCGTAGATATCGTAGATATCGTAGATATCG', 1) == 1
    assert mc.connections['kmers']['T'].getbit(
        'ATCGTAGATATCGTAGATATCGTAGATATCG', 1) == 0
    mc.delete()
    assert mc.connections['kmers']['A'].getbit(
        'ATCGTAGATATCGTAGATATCGTAGATATCG', 1) == 0
    assert mc.connections['kmers']['T'].getbit(
        'ATCGTAGATATCGTAGATATCGTAGATATCG', 1) == 0


def test_set_kmers():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    mc.set_kmers(
        ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'ATCTACAATATCTACAATATCTACAATATCT'], 1)
    assert mc.connections['kmers']['A'].getbit(
        'ATCGTAGATATCGTAGATATCGTAGATATCG', 1) == 1
    assert mc.connections['kmers']['A'].getbit(
        'AGATATTGTAGATATTGTAGATATTGTAGAT', 1) == 1
    assert mc.connections['kmers']['T'].getbit(
        'ATCGTAGATATCGTAGATATCGTAGATATCG', 1) == 0
    assert mc.connections['kmers']['T'].getbit(
        'AGATATTGTAGATATTGTAGATATTGTAGAT', 1) == 0
    mc.delete()


def test_add_kmer():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    mc.add_sample('s0')
    mc.num_colours = mc.get_num_colours()
    mc.add_kmer('ATCGTAGATATCGTAGATATCGTAGATATCG', colour=0)
    assert mc.get_kmer('ATCGTAGATATCGTAGATATCGTAGATATCG') == None
    assert mc._get_set_connection(0).sismember(
        '0', 'ATCGTAGATATCGTAGATATCGTAGATATCG') == 1
    assert mc.query_kmers(['ATCGTAGATATCGTAGATATCGTAGATATCG']) == [(0,)]
    assert mc.search_sets('ATCGTAGATATCGTAGATATCGTAGATATCG', -1) == 0
    # Add the same kmer in another colour
    mc.add_sample('s1')
    mc.num_colours = mc.get_num_colours()
    mc.add_kmer('ATCGTAGATATCGTAGATATCGTAGATATCG', colour=1)
    assert mc.get_kmer('ATCGTAGATATCGTAGATATCGTAGATATCG') != None
    assert mc._get_set_connection(1).sismember(
        '1', 'ATCGTAGATATCGTAGATATCGTAGATATCG') == 0
    assert mc.query_kmers(['ATCGTAGATATCGTAGATATCGTAGATATCG']) == [(1, 1)]
    # Add a third kmer
    mc.add_kmer('ATCGTAGATATCGTAGGGATCGTAGATATCG', colour=1)
    assert mc.get_kmer('ATCGTAGATATCGTAGGGATCGTAGATATCG') == None
    assert mc._get_set_connection(1).sismember(
        '1', 'ATCGTAGATATCGTAGGGATCGTAGATATCG') == 1
    assert mc.query_kmers(['ATCGTAGATATCGTAGGGATCGTAGATATCG']) == [(0, 0)]


def test_query_kmers():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    mc.delete()

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
    mc.delete()

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


def test_stats():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    mc.set_kmers(
        ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'ATCTACAATATCTACAATATCTACAATATCT'], 1)
    mc.count_kmers() == 1
    mc.calculate_memory() > 0
    mc.delete()


def test_kmers_to_bytes():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    for i in range(100):
        kmer = "".join([random.choice(KMERS) for _ in range(31)])
        # print(kmer, mc._kmer_to_bytes(kmer),
        #       mc._bytes_to_kmer(mc._kmer_to_bytes(kmer)))
        assert mc._bytes_to_kmer(mc._kmer_to_bytes(kmer)) == kmer


def test_samples():
    mc = McDBG(conn_config=conn_config, compress_kmers=False)
    mc.delete()
    assert mc.get_num_colours() == 0

    mc.add_sample('1234')
    mc.add_sample('1235')

    assert mc.get_sample_colour('1234') == 0
    assert mc.get_num_colours() == 2

    # mc.add_sample('1235')
    # assert mc.get_sample_colour('1235') == '1'
    # assert mc.get_num_colours() == 2
