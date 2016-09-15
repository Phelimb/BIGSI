from remcdbg import McDBG
import random
from remcdbg.utils import hash_key
conn_config = [('localhost', 6200), ('localhost', 6201),
               ('localhost', 6202), ('localhost', 6203)]
conn_config = [('localhost', 6379)]


def test_set_compress():
    mc = McDBG(conn_config=conn_config, compress_kmers=True)
    mc.flushall()
    kmer_shared = 'ATCGTAGATATCGTAGATATCGTAGATATCG'
    kmer_unique1 = 'ATCGTAGATATCGTAGATATCGTAGATATCC'
    kmer_unique2 = 'ATCGTAGATATCGTAGATATCGTAGATATCT'
    mc.add_sample('1')
    mc.add_sample('2')
    mc.set_kmers([kmer_shared, kmer_unique1], 0)
    mc.set_kmers([kmer_shared, kmer_unique2], 1)
    assert mc.query_kmers([kmer_shared, kmer_unique1, kmer_unique2]) == [
        (1, 1), (1, 0), (0, 1)]
    assert mc.count_keys() == 3
    assert mc.count_kmers_in_sets() == 0

    mc.compress_set()
    assert mc.count_keys() == 1
    assert mc.count_kmers_in_sets() == 2
    assert mc.query_kmers([kmer_shared, kmer_unique1, kmer_unique2]) == [
        (1, 1), (1, 0), (0, 1)]


def test_list_compress():
    mc = McDBG(conn_config=conn_config, compress_kmers=True)
    mc.flushall()
    kmer_shared = 'ATCGTAGATATCGTAGATATCGTAGATATCG'
    kmer_unique1 = 'ATCGTAGATATCGTAGATATCGTAGATATCC'
    kmer_unique2 = 'ATCGTAGATATCGTAGATATCGTAGATATCT'
    mc.add_sample('1')
    mc.add_sample('2')
    mc.set_kmers([kmer_shared, kmer_unique1], 0)
    mc.set_kmers([kmer_shared, kmer_unique2], 1)
    assert mc.query_kmers([kmer_shared, kmer_unique1, kmer_unique2]) == [
        (1, 1), (1, 0), (0, 1)]
    assert mc.count_keys() == 3
    assert mc.count_kmers_in_lists() == 0

    mc.compress(sparsity_threshold=0.5)
    assert mc.count_keys() == 1
    assert mc.count_kmers_in_lists() == 2
    assert mc.query_kmers([kmer_shared, kmer_unique1, kmer_unique2]) == [
        (1, 1), (1, 0), (0, 1)]

    mc.uncompress(sparsity_threshold=0)
    assert mc.query_kmers([kmer_shared, kmer_unique1, kmer_unique2]) == [
        (1, 1), (1, 0), (0, 1)]
    assert mc.count_keys() == 3
    assert mc.count_kmers_in_lists() == 0


# def test_hash_compress():
#     mc = McDBG(conn_config=conn_config, compress_kmers=True)
#     mc.flushall()
#     kmer_shared = 'ATCGTAGATATCGTAGATATCGTAGATATCG'
#     kmer_unique1 = 'ATCGTAGATATCGTAGATATCGTAGATATCC'
#     kmer_unique2 = 'ATCGTAGATATCGTAGATATCGTAGATATCT'
#     mc.add_sample('1')
#     mc.add_sample('2')
#     mc.set_kmers([kmer_shared, kmer_unique1], 0)
#     mc.set_kmers([kmer_shared, kmer_unique2], 1)
#     assert mc.query_kmers([kmer_shared, kmer_unique1, kmer_unique2]) == [
#         (1, 1), (1, 0), (0, 1)]
#     assert mc.count_keys() == 3
#     assert mc.count_kmers_in_lists() == 0
#     kmers = [k for k in mc.clusters['kmers'].scan_iter('*')]
#     # mc.compress_list(sparsity_threshold=0.5)
#     mc.compress_hash()
#     # tests? TODO
#     kmer = kmers[0]
#     assert mc.clusters['kmers'].hget(hash_key(kmer), kmer) is not None
