from remcdbg import McDBG
import random
conn_config = [('localhost', 6200), ('localhost', 6201),
               ('localhost', 6202), ('localhost', 6203)]


def test_set_compress():
    mc = McDBG(conn_config=conn_config, compress_kmers=True)
    mc.delete()
    kmer_shared = 'ATCGTAGATATCGTAGATATCGTAGATATCG'
    kmer_unique1 = 'ATCGTAGATATCGTAGATATCGTAGATATCC'
    kmer_unique2 = 'ATCGTAGATATCGTAGATATCGTAGATATCT'
    mc.add_sample('1')
    mc.add_sample('2')
    mc.set_kmers([kmer_shared, kmer_unique1], 0)
    mc.set_kmers([kmer_shared, kmer_unique2], 1)
    assert mc.query_kmers([kmer_shared, kmer_unique1, kmer_unique2]) == [
        (1, 1), (1, 0), (0, 1)]
    assert mc.count_kmers() == 3
    assert mc.count_kmers_in_sets() == 0

    mc.compress()
    assert mc.count_kmers() == 1
    assert mc.count_kmers_in_sets() == 2
    assert mc.query_kmers([kmer_shared, kmer_unique1, kmer_unique2]) == [
        (1, 1), (1, 0), (0, 1)]


def test_list_compress():
    mc = McDBG(conn_config=conn_config, compress_kmers=True)
    mc.delete()
    kmer_shared = 'ATCGTAGATATCGTAGATATCGTAGATATCG'
    kmer_unique1 = 'ATCGTAGATATCGTAGATATCGTAGATATCC'
    kmer_unique2 = 'ATCGTAGATATCGTAGATATCGTAGATATCT'
    mc.add_sample('1')
    mc.add_sample('2')
    mc.set_kmers([kmer_shared, kmer_unique1], 0)
    mc.set_kmers([kmer_shared, kmer_unique2], 1)
    assert mc.query_kmers([kmer_shared, kmer_unique1, kmer_unique2]) == [
        (1, 1), (1, 0), (0, 1)]
    assert mc.count_kmers() == 3
    assert mc.count_kmers_in_lists() == 0

    mc.compress_list(sparsity_threshold=0.5)
    assert mc.count_kmers() == 1
    assert mc.count_kmers_in_lists() == 2
    assert mc.query_kmers([kmer_shared, kmer_unique1, kmer_unique2]) == [
        (1, 1), (1, 0), (0, 1)]
