from remcdbg.main import McDBG

ports = [6300, 6301, 6302, 6303, 6304, 6305, 6306, 6307, 6308, 6309, 6310, 6311,
         6312, 6313, 6314, 6315, 6316, 6317, 6318, 6319, 6320, 6321, 6322, 6323,
         6324, 6325, 6326, 6327, 6328, 6329, 6330, 6331, 6332, 6333, 6334, 6335,
         6336, 6337, 6338, 6339, 6340, 6341, 6342, 6343, 6344, 6345, 6346, 6347,
         6348, 6349, 6350, 6351, 6352, 6353, 6354, 6355, 6356, 6357, 6358, 6359,
         6360, 6361, 6362, 6363]


def test_init():
    mc = McDBG(ports=ports)
    assert len(mc.connections) == 3
    assert len(mc.connections['kmers']) == 64


def test_add_kmer():
    mc = McDBG(ports=ports)
    mc.set_kmer('ATCGTAGAT', 1)
    print(mc.connections['kmers']['ATC'].get('ATCGTAGAT'))
    assert mc.connections['kmers']['ATC'].get('ATCGTAGAT') == '1'
    assert mc.connections['kmers']['ATT'].get('ATCGTAGAT') is None
    mc.delete()
    assert mc.connections['kmers']['ATC'].get('ATCGTAGAT') is None
    assert mc.connections['kmers']['ATT'].get('ATCGTAGAT') is None


def test_set_cbit():
    mc = McDBG(ports=ports)
    mc.set_colour(1, 0)

    assert mc.connections['colours'][1].getbit(1, 0) == 1
    assert mc.connections['colours'][2].getbit(1, 0) == 0
    assert mc.connections['colours'][0].getbit(64, 1) == 0

    mc.set_colour(64, 1)
    assert mc.connections['colours'][0].getbit(64, 1) == 1
    assert mc.connections['colours'][2].getbit(64, 1) == 0

    mc.delete()
    assert mc.connections['colours'][1].getbit(1, 0) == 0
    assert mc.connections['colours'][2].getbit(1, 0) == 0
    assert mc.connections['colours'][0].getbit(64, 1) == 0
    assert mc.connections['colours'][2].getbit(64, 1) == 0


def test_samples():
    mc = McDBG(ports=ports)
    assert mc.num_colours == 0

    mc.add_sample('1234')
    assert mc.get_sample_colour('1234') == '0'
    assert mc.num_colours == 1

    mc.add_sample('1235')
    assert mc.get_sample_colour('1235') == '1'
    assert mc.num_colours == 2
