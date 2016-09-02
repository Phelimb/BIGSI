from remcdbg.mcdbg import McDBG
import random
ports = [6200, 6201, 6202, 6203]
KMERS = ['A', 'T', 'C', 'G']


def test_init():
    mc = McDBG(ports=ports, compress_kmers=False)
    assert len(mc.connections) == 3
    assert len(mc.connections['kmers']) == 4


def test_add_kmer():
    mc = McDBG(ports=ports, compress_kmers=False)
    mc.set_kmer('ATCGTAGAT', 1)
    print(mc.connections['kmers']['A'].getbit('ATCGTAGAT', 1))
    assert mc.connections['kmers']['A'].getbit('ATCGTAGAT', 1) == 1
    assert mc.connections['kmers']['T'].getbit('ATCGTAGAT', 1) == 0
    mc.delete()
    assert mc.connections['kmers']['A'].getbit('ATCGTAGAT', 1) == 0
    assert mc.connections['kmers']['T'].getbit('ATCGTAGAT', 1) == 0


def test_add_kmers():
    mc = McDBG(ports=ports, compress_kmers=False)
    mc.set_kmers(['ATCGTAGAT', 'ATCTACAAT'], 1)
    assert mc.connections['kmers']['A'].getbit('ATCGTAGAT', 1) == 1
    assert mc.connections['kmers']['A'].getbit('ATCTACAAT', 1) == 1
    assert mc.connections['kmers']['T'].getbit('ATCGTAGAT', 1) == 0
    assert mc.connections['kmers']['T'].getbit('ATCTACAAT', 1) == 0
    mc.delete()


def test_query_kmers():
    mc = McDBG(ports=ports, compress_kmers=False)
    mc.delete()

    mc.add_sample('1234')
    mc.add_sample('1235')
    mc.add_sample('1236')

    mc.set_kmers(['ATCGTAGAT', 'ATCTACAAT'], 0)
    mc.set_kmers(['ATCGTAGAT', 'ATTGTAGAG'], 1)
    mc.set_kmers(['ATCGTAGAC', 'ATTGTAGAG'], 2)
    assert mc.get_num_colours() == 3
    mc.num_colours = mc.get_num_colours()
    assert mc.query_kmers(['ATCGTAGAT', 'ATCTACAAT']) == [
        (1, 1, 0), (1, 0, 0)]
    mc.delete()

    mc.add_sample('1234')
    mc.add_sample('1235')
    mc.add_sample('1236')

    mc.set_kmers(['ATCGTAGAT', 'CTTGTAGAT'], 0)
    mc.set_kmers(['ATCGTAGAT', 'ATTGTAGAG'], 1)
    mc.set_kmers(['ATCGTAGAC', 'ATTGTAGAG'], 2)
    assert mc.query_kmers(['ATCGTAGAT', 'CTTGTAGAT']) == [
        (1, 1, 0), (1, 0, 0)]


def test_stats():
    mc = McDBG(ports=ports, compress_kmers=False)
    mc.set_kmers(['ATCGTAGAT', 'ATCTACAAT'], 1)
    mc.count_kmers() == 1
    mc.calculate_memory() > 0
    mc.delete()


def test_kmers_to_hex():
    mc = McDBG(ports=ports, compress_kmers=False)
    assert mc._kmer_to_bytes('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') == '\x00'*8
    assert mc._bytes_to_kmer('\x00'*8) == 'A'*31
    for i in range(100):
        kmer = "".join([random.choice(KMERS) for _ in range(31)])
        assert mc._bytes_to_kmer(mc._kmer_to_bytes(kmer)) == kmer


# def test_set_cbit():
#     mc = McDBG(ports=ports, compress_kmers=False)
#     mc.set_colour(1, 0)

#     assert mc.connections['colours'][1].getbit(1, 0) == 1
#     assert mc.connections['colours'][2].getbit(1, 0) == 0
#     assert mc.connections['colours'][0].getbit(64, 1) == 0

#     mc.set_colour(64, 1)
#     assert mc.connections['colours'][0].getbit(64, 1) == 1
#     assert mc.connections['colours'][2].getbit(64, 1) == 0

#     mc.delete()
#     assert mc.connections['colours'][1].getbit(1, 0) == 0
#     assert mc.connections['colours'][2].getbit(1, 0) == 0
#     assert mc.connections['colours'][0].getbit(64, 1) == 0
#     assert mc.connections['colours'][2].getbit(64, 1) == 0


def test_samples():
    mc = McDBG(ports=ports, compress_kmers=False)
    assert mc.get_num_colours() == 0

    mc.add_sample('1234')
    mc.add_sample('1235')

    assert mc.get_sample_colour('1234') == '0'
    assert mc.get_num_colours() == 2

    # mc.add_sample('1235')
    # assert mc.get_sample_colour('1235') == '1'
    # assert mc.get_num_colours() == 2
