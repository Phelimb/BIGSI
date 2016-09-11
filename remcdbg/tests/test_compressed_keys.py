from remcdbg import McDBG
from remcdbg import kmer_to_bits
import random
from remcdbg.utils import seq_to_kmers

conn_config = [('localhost', 6200), ('localhost', 6201),
               ('localhost', 6202), ('localhost', 6203)]
KMERS = ['A', 'T', 'C', 'G']


def test_add_kmer():
    mc = McDBG(conn_config=conn_config, compress_kmers=True)
    kmer = 'ATCGTAGATATCGTAGATATCGTAGATATCG'
    kmer_rc = 'CGATATCTACGATATCTACGATATCTACGAT'
    # print(mc._kmer_to_bytes(kmer_rc))
    bitstring = kmer_to_bits(kmer)
    mc.set_kmer(kmer, 1)
    _bytes = b'6\xc8\xcd\xb23l\x8c\xd8'
    _bytes_rc = b'c7\x18\xcd\xc63q\x8c'
    assert mc.clusters['kmers'].getbit(
        _bytes, 1) == 1 or mc.clusters['kmers'].getbit(
        _bytes_rc, 1)
    assert mc.clusters['kmers'].getbit(
        _bytes, 1) == 0
    mc.flushall()
    assert mc.clusters['kmers'].getbit(
        _bytes, 1) == 0
    assert mc.clusters['kmers'].getbit(
        _bytes, 1) == 0


def test_add_kmers():
    mc = McDBG(conn_config=conn_config, compress_kmers=True)
    k1 = 'ATCGTAGATATCGTAGATATCGTAGATATCG'
    k2 = 'AGATATTGTAGATATTGTAGATATTGTAGAT'
    k1_rc = 'CGATATCTACGATATCTACGATATCTACGAT'
    k2_rc = 'ATCTACAATATCTACAATATCTACAATATCT'

    mc.set_kmers(
        [k1, k2], 1)
    _bytes = b'#>\xc8\xcf\xb23\xec\x8c'
    _bytes2 = b'6\xc8\xcd\xb23l\x8c\xd8'
    _bytes1_rc = b'c7\x18\xcd\xc63q\x8c'
    _bytes2_rc = b'7\x10\xcd\xc43q\x0c\xdc'
    assert mc.clusters['kmers'].getbit(
        _bytes, 1) == 1 or mc.clusters['kmers'].getbit(_bytes1_rc, 1) == 1
    assert mc.clusters['kmers'].getbit(
        _bytes2, 1) == 1 or mc.clusters['kmers'].getbit(
        _bytes2_rc, 1)
    assert mc.clusters['kmers'].getbit(
        _bytes, 1) == 0
    assert mc.clusters['kmers'].getbit(
        _bytes2, 1) == 0
    mc.flushall()


def test_query_kmers():
    mc = McDBG(conn_config=conn_config, compress_kmers=True)
    mc.flushall()

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
    mc.flushall()

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

    assert mc.query_kmers_100_per(['ATCGTAGATATCGTAGATATCGTAGATATCG']) == [
        True, True, False]
    assert mc.query_kmers_100_per(['ATCGTAGATATCGTAGATATCGTAGATATCG', 'CTTGTAGATCTTGTAGATCTTGTAGATCTTG']) == [
        True, False, False]
    mc.flushall()


def test_query_kmers_2():
    mc = McDBG(conn_config=conn_config, compress_kmers=True)
    mc.flushall()
    mc.add_sample('1234')
    kmer = "TAGATCAGAAAACCATATTCAAATGGGATAA"
    mc.set_kmers(
        [kmer], 0)
    assert mc.query_kmers_100_per([kmer]) == [
        True]


def test_query_kmers_3():
    mc = McDBG(conn_config=conn_config, compress_kmers=True)
    gene = "ATGAAAAACACAATACATATCAACTTCGCTATTTTTTTAATAATTGCAAATATTATCTACAGCAGCGCCAGTGCATCAACAGATATCTCTACTGTTGCATCTCCATTATTTGAAGGAACTGAAGGTTGTTTTTTACTTTACGATGCATCCACAAACGCTGAAATTGCTCAATTCAATAAAGCAAAGTGTGCAACGCAAATGGCACCAGATTCAACTTTCAAGATCGCATTATCACTTATGGCATTTGATGCGGAAATAATAGATCAGAAAACCATATTCAAATGGGATAAAACCCCCAAAGGAATGGAGATCTGGAACAGCAATCATACACCAAAGACGTGGATGCAATTTTCTGTTGTTTGGGTTTCGCAAGAAATAACCCAAAAAATTGGATTAAATAAAATCAAGAATTATCTCAAAGATTTTGATTATGGAAATCAAGACTTCTCTGGAGATAAAGAAAGAAACAACGGATTAACAGAAGCATGGCTCGAAAGTAGCTTAAAAATTTCACCAGAAGAACAAATTCAATTCCTGCGTAAAATTATTAATCACAATCTCCCAGTTAAAAACTCAGCCATAGAAAACACCATAGAGAACATGTATCTACAAGATCTGGATAATAGTACAAAACTGTATGGGAAAACTGGTGCAGGATTCACAGCAAATAGAACCTTACAAAACGGATGGTTTGAAGGGTTTATTATAAGCAAATCAGGACATAAATATGTTTTTGTGTCCGCACTTACAGGAAACTTGGGGTCGAATTTAACATCAAGCATAAAAGCCAAGAAAAATGCGATCACCATTCTAAACACACTAAATTTATAA"
    mc.flushall()
    kmers = [k for k in seq_to_kmers(gene)]
    mc.add_sample('1234')
    mc.set_kmers(
        kmers, 0)
    assert mc.query_kmers_100_per(kmers) == [
        True]
