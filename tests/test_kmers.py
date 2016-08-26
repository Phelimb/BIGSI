from remcdbg.mcdbg import McDBG

ports = [6200, 6201, 6202, 6203]


def test_init():
    mc = McDBG(ports=ports)
    assert len(mc.connections) == 3
    assert len(mc.connections['kmers']) == 4


def test_add_kmer():
    mc = McDBG(ports=ports)
    mc.set_kmer('ATCGTAGAT', 1)
    print(mc.connections['kmers']['A'].getbit('ATCGTAGAT', 1))
    assert mc.connections['kmers']['A'].getbit('ATCGTAGAT', 1) == 1
    assert mc.connections['kmers']['T'].getbit('ATCGTAGAT', 1) == 0
    mc.delete()
    assert mc.connections['kmers']['A'].getbit('ATCGTAGAT', 1) == 0
    assert mc.connections['kmers']['T'].getbit('ATCGTAGAT', 1) == 0


def test_add_kmers():
    mc = McDBG(ports=ports)
    mc.set_kmers(['ATCGTAGAT', 'ATTGTAGAT'], 1)
    assert mc.connections['kmers']['A'].getbit('ATCGTAGAT', 1) == 1
    assert mc.connections['kmers']['A'].getbit('ATTGTAGAT', 1) == 1
    assert mc.connections['kmers']['T'].getbit('ATCGTAGAT', 1) == 0
    assert mc.connections['kmers']['T'].getbit('ATTGTAGAT', 1) == 0
    mc.delete()


def test_query_kmers():
    mc = McDBG(ports=ports)

    mc.add_sample('1234')
    mc.add_sample('1235')
    mc.add_sample('1236')

    mc.set_kmers(['ATCGTAGAT', 'ATTGTAGAT'], 0)
    mc.set_kmers(['ATCGTAGAT', 'ATTGTAGAG'], 1)
    mc.set_kmers(['ATCGTAGAC', 'ATTGTAGAG'], 2)
    assert mc.num_colours == 3
    assert mc.query_kmers(['ATCGTAGAT', 'ATTGTAGAT']) == [
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
    mc = McDBG(ports=ports)
    mc.set_kmers(['ATCGTAGAT', 'ATTGTAGAT'], 1)
    mc.count_kmers() == 1
    mc.calculate_memory() > 0
    mc.delete()


# def test_set_cbit():
#     mc = McDBG(ports=ports)
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
    mc = McDBG(ports=ports)
    assert mc.num_colours == 0

    mc.add_sample('1234')
    mc.add_sample('1235')

    assert mc.get_sample_colour('1234') == '0'
    assert mc.num_colours == 2

    # mc.add_sample('1235')
    # assert mc.get_sample_colour('1235') == '1'
    # assert mc.num_colours == 2
