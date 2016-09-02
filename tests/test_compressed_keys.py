from remcdbg.mcdbg import McDBG
from remcdbg.utils import kmer_to_bits
import random
ports = [6200, 6201, 6202, 6203]
KMERS = ['A', 'T', 'C', 'G']


def test_add_kmer():
    mc = McDBG(ports=ports, compress_kmers=True)
    kmer = 'AGAGATATAGACTTATTAAAAAATACAATAT'
    bitstring = kmer_to_bits(kmer)
    print(bitstring)
    print([mc.connections['kmers']['A'].setbit('tmp', int(i), int(j))
           for i, j in enumerate(bitstring)])
    print(mc.connections['kmers']['A'].get('tmp'))
    # mc._kmer_to_bytes()
    mc.set_kmer('AGAGATATAGACTTATTAAAAAATACAATAT', 1)
    _bytes = b'6\xc8\xcd\xb23l\x8c\xd8'
    print(mc.connections['kmers']['A'].getbit(
        _bytes, 1))
    assert mc.connections['kmers']['A'].getbit(
        _bytes, 1) == 1
    assert mc.connections['kmers']['T'].getbit(
        _bytes, 1) == 0
    # mc.delete()
    # assert mc.connections['kmers']['A'].getbit(
    #     _bytes, 1) == 0
    # assert mc.connections['kmers']['T'].getbit(
    #     _bytes, 1) == 0


def test_byte_encode():
    mc = McDBG(ports=ports, compress_kmers=True)
    # print(list(mc._bytestring_encode(
    #     'S\x8a\xf9\x96"\xe3\x0c\xd8')))
    assert mc._bytestring_encode(
        'S\x8a\xf9\x96"\xe3\x0c\xd8') == '"S\x8a\xf9\x96\"\xe3\x0c\xd8"'
    # print(list(mc._bytestring_encode(
    #     "\x86\xad\xfd\x16\xd9m'\x90")))
    assert mc._bytestring_encode(
        "\x86\xad\xfd\x16\xd9m'\x90") == '"\x86\xad\xfd\x16\xd9m\'\x90"'
    assert mc._bytestring_encode(
        "f\x8e\xb9P\xe6\xd9 0") == '"f\x8e\xb9P\xe6\xd9\ 0"'
    print(list(mc._byte_encode(b'"@\tz:\'\xcd\x9c')))
    print(list('"@\tz:\'\xcd\x9c'))
    assert mc._byte_encode(b'"@\tz:\'\xcd\x9c') == '\"@\tz:\'\xcd\x9c'
# def test_add_kmers():
#     mc = McDBG(ports=ports, compress_kmers=True)
#     mc.set_kmers(
#         ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'AGATATTGTAGATATTGTAGATATTGTAGAT'], 1)
#     _bytes = "".join(['6', '\xc8', '\xcd', '\xb2', '3', 'l', '\x8c', '\xd8'])
#     # print(list(mc._kmer_to_bytes('AGATATTGTAGATATTGTAGATATTGTAGAT')))
# _bytes2 = "".join(['#', '>', '\xc8', '\xcf', '\xb2', '3', '\xec',
# '\x8c'])

#     assert mc.connections['kmers']['A'].getbit(
#         _bytes, 1) == 1
#     assert mc.connections['kmers']['A'].getbit(
#         _bytes2, 1) == 1
#     # assert mc.connections['kmers']['A'].getbit(
#     #     _bytes, 1) == 0
#     # assert mc.connections['kmers']['A'].getbit(
#     #     _bytes2, 1) == 0
#     mc.delete()


# def test_query_kmers():
#     mc = McDBG(ports=ports, compress_kmers=True)
#     mc.delete()

#     mc.add_sample('1234')
#     mc.add_sample('1235')
#     mc.add_sample('1236')

#     mc.set_kmers(
#         ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'ATCTACAATATCTACAATATCTACAATATCT'], 0)
#     mc.set_kmers(
#         ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'ATTGTAGAGATTGTAGAGATTGTAGAGATTA'], 1)
#     mc.set_kmers(
#         ['ATCGTAGACATCGTAGACATCGTAGACATCG', 'ATTGTAGAGATTGTAGAGATTGTAGAGATTA'], 2)
#     assert mc.get_num_colours() == 3
#     mc.num_colours = mc.get_num_colours()
#     assert mc.query_kmers(['ATCGTAGATATCGTAGATATCGTAGATATCG', 'ATCTACAATATCTACAATATCTACAATATCT']) == [
#         (1, 1, 0), (1, 0, 0)]
#     mc.delete()

#     mc.add_sample('1234')
#     mc.add_sample('1235')
#     mc.add_sample('1236')

#     mc.set_kmers(
#         ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'CTTGTAGATCTTGTAGATCTTGTAGATCTTG'], 0)
#     mc.set_kmers(
#         ['ATCGTAGATATCGTAGATATCGTAGATATCG', 'ATTGTAGAGATTGTAGAGATTGTAGAGATTA'], 1)
#     mc.set_kmers(
#         ['ATCGTAGACATCGTAGACATCGTAGACATCG', 'ATTGTAGAGATTGTAGAGATTGTAGAGATTA'], 2)
#     assert mc.query_kmers(['ATCGTAGATATCGTAGATATCGTAGATATCG', 'CTTGTAGATCTTGTAGATCTTGTAGATCTTG']) == [
#         (1, 1, 0), (1, 0, 0)]
