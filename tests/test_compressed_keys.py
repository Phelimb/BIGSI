from remcdbg.mcdbg import McDBG
import random
ports = [6200, 6201, 6202, 6203]
KMERS = ['A', 'T', 'C', 'G']


def test_add_kmer():
    mc = McDBG(ports=ports, compress_kmers=True)
    mc.set_kmer('ATCGTAGATATCGTAGATATCGTAGATATCG', 1)
    _bytes = "".join(['6', '\xc8', '\xcd', '\xb2', '3', 'l', '\x8c', '\xd8'])
    print(mc.connections['kmers']['A'].getbit(
        _bytes, 1))
    assert mc.connections['kmers']['A'].getbit(
        _bytes, 1) == 1
    assert mc.connections['kmers']['T'].getbit(
        _bytes, 1) == 0
    mc.delete()
    assert mc.connections['kmers']['A'].getbit(
        _bytes, 1) == 0
    assert mc.connections['kmers']['T'].getbit(
        _bytes, 1) == 0


def test_add_kmers():
    mc = McDBG(ports=ports, compress_kmers=True)
    mc.set_kmers(
        ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'AGATATTGTAGATATTGTAGATATTGTAGAT'], 1)
    _bytes = "".join(['6', '\xc8', '\xcd', '\xb2', '3', 'l', '\x8c', '\xd8'])
    # print(list(mc._kmer_to_bytes('AGATATTGTAGATATTGTAGATATTGTAGAT')))
    _bytes2 = "".join(['#', '>', '\xc8', '\xcf', '\xb2', '3', '\xec', '\x8c'])

    assert mc.connections['kmers']['A'].getbit(
        _bytes, 1) == 1
    assert mc.connections['kmers']['A'].getbit(
        _bytes2, 1) == 1
    # assert mc.connections['kmers']['A'].getbit(
    #     _bytes, 1) == 0
    # assert mc.connections['kmers']['A'].getbit(
    #     _bytes2, 1) == 0
    mc.delete()


def test_query_kmers():
    mc = McDBG(ports=ports, compress_kmers=True)
    mc.delete()

    mc.add_sample('1234')
    mc.add_sample('1235')
    mc.add_sample('1236')

    mc.set_kmers(
        ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'ATCTACAATATCTACAATATCTACAATATCT'], 0)
    mc.set_kmers(
        ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'ATTGTAGAGATTGTAGAGATTGTAGAGATTA'], 1)
    mc.set_kmers(
        ['ATCGTAGACATCGTAGACATCGTAGACATCG', 'ATTGTAGAGATTGTAGAGATTGTAGAGATTA'], 2)
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
    mc.set_kmers(
        ['ATCGTAGACATCGTAGACATCGTAGACATCG', 'ATTGTAGAGATTGTAGAGATTGTAGAGATTA'], 2)
    assert mc.query_kmers(['ATCGTAGATATCGTAGATATCGTAGATATCG', 'CTTGTAGATCTTGTAGATCTTGTAGATCTTG']) == [
        (1, 1, 0), (1, 0, 0)]
