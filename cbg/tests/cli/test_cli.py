import os
import hug
import redis

import cbg.__main__
import json
from cbg.tests.base import ST_SEQ
from cbg.tests.base import ST_KMER
from cbg.tests.base import ST_SAMPLE_NAME
from cbg.tests.base import ST_GRAPH
from cbg.tests.base import ST_STORAGE
import hypothesis.strategies as st
from hypothesis import given
import random
import tempfile
from cbg.utils import seq_to_kmers
from bitarray import bitarray
import numpy as np


def test_bloom_cmd():
    f = '/tmp/test_kmers.bloom'
    response = hug.test.post(
        cbg.__main__, 'bloom', {'ctx': 'cbg/tests/data/test_kmers.ctx', 'outfile': f})
    a = bitarray()
    with open(f, 'rb') as inf:
        a.fromfile(inf)
    assert sum(a) > 0

    os.remove(f)


def load_bloomfilter(f):
    bloomfilter = bitarray()
    with open(f, 'rb') as inf:
        bloomfilter.fromfile(inf)
    return np.array(bloomfilter)

import string


def test_build_cmd():
    f = '/tmp/data'
    response = hug.test.delete(cbg.__main__, '', {'db': f})
    N = 3
    bloomfilter_filepaths = ['cbg/tests/data/test_kmers.bloom']*N
    samples = []
    for i in range(N):
        samples.append(''.join(random.choice(
            string.ascii_uppercase + string.digits) for _ in range(6)))
    response = hug.test.post(
        cbg.__main__, 'build', {'bloomfilters': bloomfilter_filepaths,
                                'outfile': f, 'samples': samples, 'bloom_filter_size': 1000})
    # TODO fix below
    seq = 'GATCGTTTGCGGCCACAGTTGCCAGAGATGA'
    response = hug.test.get(cbg.__main__, 'search', {'db': f, 'seq': seq})
    assert response.data.get(seq).get('results')
    assert "score" in list(response.data.get(seq).get('results').values())[0]
    seq = 'GATCGTTTGCGGCCACAGTTGCCAGAGATGAAAG'
    response = hug.test.get(cbg.__main__, 'search', {
                            'db': f, 'seq': seq, 'threshold': 0.1})
    print(response.data)
    assert response.data.get(seq).get('results')
    assert "score" in list(response.data.get(seq).get('results').values())[0]
    response = hug.test.delete(
        cbg.__main__, '', {'db': f, })


# TODO, insert takes a bloom filters
# def test_insert_from_merge_and_search_cmd():
#     # Returns a Response object
#     response = hug.test.delete(
#         cbg.__main__, '', {})
#     assert not '404' in response.data
#     response = hug.test.post(
#         cbg.__main__, 'insert', {'merge_results': 'cbg/tests/data/merge/test_merge_resuts.json', 'force': True})
#     seq = 'GATCGTTTGCGGCCACAGTTGCCAGAGATGA'
#     response = hug.test.get(cbg.__main__, 'search', {'seq': seq})
#     for i in range(1, 6):
#         assert response.data.get(seq).get(
#             'results').get('cbg/tests/data/test_kmers.bloom%i' % i) == 1.0
#     assert response.data.get(seq).get(
#         'results').get('cbg/tests/data/test_kmers.bloom') == 1.0
#     # response = hug.test.delete(
#     #     cbg.__main__, '', {})

# TODO, insert takes a bloom filters
# def test_insert_search_cmd():
#     # Returns a Response object
#     response = hug.test.delete(
#         cbg.__main__, '', {})
#     assert not '404' in response.data
#     response = hug.test.post(
#         cbg.__main__, 'insert', {'kmer_file': 'cbg/tests/data/test_kmers.txt'})
#     # assert response.data.get('result') == 'success'
#     seq = 'GATCGTTTGCGGCCACAGTTGCCAGAGATGA'
#     response = hug.test.get(cbg.__main__, 'search', {'seq': seq})
#     assert response.data.get(seq).get(
#         'results').get('test_kmers') == 1.0
#     response = hug.test.delete(
#         cbg.__main__, '', {})

# TODO, insert takes a bloom filters
# def test_insert_search_cmd_ctx():
#     # Returns a Response object
#     response = hug.test.delete(
#         cbg.__main__, '', {})
#     assert not '404' in response.data
#     response = hug.test.post(
#         cbg.__main__, 'insert', {'ctx': 'cbg/tests/data/test_kmers.ctx'})
#     # assert response.data.get('result') == 'success'
#     seq = 'GATCGTTTGCGGCCACAGTTGCCAGAGATGA'
#     response = hug.test.get(
#         cbg.__main__, 'search', {'seq': 'GATCGTTTGCGGCCACAGTTGCCAGAGATGA'})

#     assert response.data.get(seq).get(
#         'results').get('test_kmers') == 1.0
#     response = hug.test.delete(
#         cbg.__main__, '', {})

# TODO, insert takes a bloom filters
# @given(store=ST_STORAGE, sample=ST_SAMPLE_NAME,
#        seq=ST_SEQ)
# def test_insert_search_cmd_2(store, sample, seq):
#     kmers = list(seq_to_kmers(seq))
#     # Returns a Response object
#     response = hug.test.delete(
#         cbg.__main__, '', {})
#     assert not '404' in response.data
#     response = hug.test.post(
#         cbg.__main__, 'insert', {'sample': sample, 'kmers': kmers})
#     # assert response.data.get('result') == 'success'
#     seq = random.choice(kmers)
#     response = hug.test.get(
#         cbg.__main__, 'search', {'seq': seq})
#     print(response.data)
#     assert response.data.get(seq).get('results').get(sample) == 1.0
#     response = hug.test.delete(
#         cbg.__main__, '', {})

# TODO, fix this test.
# def test_dump_load_cmd():
#     kmers = ["ATTTCATTTCATTTCATTTCATTTCATTTCT",
#              "CTTTACTTTACTTTACTTTACTTTACTTTAG"]
#     sample = "sample1"
#     # Returns a Response object
#     response = hug.test.delete(
#         cbg.__main__, '', {})
#     assert not '404' in response.data
#     response = hug.test.post(
#         cbg.__main__, 'insert', {'sample': sample, 'kmers': kmers})

#     # assert response.data.get('result') == 'success'
#     # Dump graph
#     _, fp = tempfile.mkstemp()
#     response = hug.test.post(
#         cbg.__main__, 'dump', {'filepath': fp})
#     assert response.data.get('result') == 'success'

#     # Delete data
#     response = hug.test.delete(
#         cbg.__main__, '', {})
#     # Load graph
#     response = hug.test.post(
#         cbg.__main__, 'load', {'filepath': fp})
#     assert response.data.get('result') == 'success'

#     # test get
#     seq = random.choice(kmers)
#     response = hug.test.get(
#         cbg.__main__, 'search', {'seq': seq})
#     assert response.data.get(seq).get('results').get(sample) == 1.0
#     response = hug.test.delete(
#         cbg.__main__, '', {})


# @given(store=ST_STORAGE, samples=st.lists(ST_SAMPLE_NAME, min_size=1, max_size=5),
#        seq=ST_SEQ)
# def test_samples_cmd(store, samples, seq):
#     kmers = list(seq_to_kmers(seq))
#     # Returns a Response object
#     response = hug.test.delete(
#         cbg.__main__, '', {})
#     assert not '404' in response.data
#     for sample in set(samples):
#         response = hug.test.post(
#             cbg.__main__, 'insert', {'sample': sample, 'kmers': kmers})
#         # assert response.data.get('result') == 'success'
#     response = hug.test.get(
#         cbg.__main__, 'samples', {})
#     for sample, sample_dict in response.data.items():
#         assert sample_dict.get("name") in samples
#         assert sample_dict.get("colour") in range(len(samples))
#         # assert abs(sample_dict.get("kmer_count") - len(kmers)) / \
#         #     len(kmers) <= 0.1
#     _name = random.choice(samples)
#     response = hug.test.get(
#         cbg.__main__, 'samples', {"name": _name})
#     assert response.data.get(_name).get("name") == _name
#     response = hug.test.delete(
#         cbg.__main__, '', {})


# def chunks(l, n):
#     """Yield successive n-sized chunks from l."""
#     if n > 0:
#         for i in range(0, len(l), n):
#             yield l[i:i + n]
#     else:
#         yield l


# @given(store=ST_STORAGE, samples=st.lists(ST_SAMPLE_NAME, min_size=2, max_size=5, unique=True),
#        kmers=st.lists(ST_KMER, min_size=10, max_size=20, unique=True))
# def test_graph_stats_cmd(store, samples, kmers):
#     N = len(kmers)/len(samples)
#     kmersl = list(chunks(kmers, int(N)))

#     samples = set(samples)
#     # Returns a Response object
#     response = hug.test.delete(
#         cbg.__main__, '', {})
#     response = hug.test.get(
#         cbg.__main__, 'graph', {})
#     # assert response.data.get("kmer_count") == 0
#     assert not '404' in response.data
#     for i, sample in enumerate(samples):
#         response = hug.test.post(
#             cbg.__main__, 'insert', {'sample': sample, 'kmers': kmersl[i]})
#         # assert response.data.get('result') == 'success'
#     response = hug.test.get(
#         cbg.__main__, 'graph', {})
#     assert response.data.get("num_samples") == len(samples)
#     # assert abs(response.data.get(
#     #     "kmer_count") - len(set(kmers))) <= 5
#     response = hug.test.delete(
#         cbg.__main__, '', {})
