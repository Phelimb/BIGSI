"""Tests that are common to graphs"""
import random
import bsddb3
from bigsi.tests.base import ST_KMER
from bigsi.tests.base import ST_SEQ
from bigsi.tests.base import ST_SAMPLE_NAME
from bigsi.tests.base import ST_GRAPH
from bigsi.tests.base import ST_KMER_SIZE
from bigsi.tests.base import ST_BLOOM_FILTER_SIZE
from bigsi.tests.base import ST_NUM_HASHES
from bigsi.utils import make_hash
from bigsi.utils import reverse_comp
from hypothesis import given
from hypothesis import example
from hypothesis import settings
import hypothesis.strategies as st
from bigsi.bytearray import ByteArray
import os
import pytest
from bigsi.utils import seq_to_kmers
from bigsi import BIGSI

from bitarray import bitarray

import os
from stat import S_IREAD, S_IRGRP, S_IROTH, S_IWUSR
import logging
logging.basicConfig()

logger = logging.getLogger(__name__)
from bigsi.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)


@given(Graph=ST_GRAPH)
def test_create(Graph):
    bigsi = Graph.create(m=100, force=True)
    assert bigsi.kmer_size == 31
    assert os.path.isdir("db-bigsi")
    bigsi.delete_all()


@given(Graph=ST_GRAPH)
def test_force_create(Graph):
    bigsi = Graph.create(force=True)
    with pytest.raises(FileExistsError):
        Graph.create(m=100, force=False)
    bigsi = Graph.create(force=True)
    assert bigsi.kmer_size == 31
    assert os.path.isdir("db-bigsi")
    bigsi.delete_all()


@given(Graph=ST_GRAPH, sample=ST_SAMPLE_NAME)
@settings(max_examples=5)
def test_add_sample_metadata(Graph, sample):
    bigsi = Graph.create(m=100, force=True)
    assert os.path.isdir("db-bigsi")
    colour = bigsi._add_sample(sample)
    assert bigsi.sample_to_colour(sample) == colour
    assert bigsi.colour_to_sample(colour) == sample
    # assert bigsi.metadata_hgetall("colour0") == sample
    # assert bigsi.metadata_hget("colours", colour) == sample
    bigsi.delete_all()


# @given(Graph=ST_GRAPH, sample=ST_SAMPLE_NAME)
# @example(Graph=BIGSI, sample='0')
# @settings(max_examples=5)
def test_insert_and_unique_sample_names():
    Graph, sample = BIGSI, '0'
    seq, k, h = 'AATTTTTATTTTTTTTTTTTTAATTAATATT', 11, 1
    m = 10
    logger.debug("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
    kmers = seq_to_kmers(seq, k)
    bigsi = Graph.create(m=m, k=k, h=h, force=True)
    assert bigsi.kmer_size == k
    bloom = bigsi.bloom(kmers)
    assert len(bloom) == m
    with pytest.raises(ValueError):
        bigsi.insert(bloom, sample)
    bigsi.build([bloom], [sample])
    with pytest.raises(ValueError):
        bigsi.insert(bloom, sample)
    assert sample in bigsi.search(seq)
    assert bigsi.search(seq).get(sample).get('percent_kmers_found') == 100
    bigsi.delete_all()


def test_cant_write_to_read_only_index():
    Graph, sample = BIGSI, "sfewe"
    seq, k, h = 'AATTTTTATTTTTTTTTTTTTAATTAATATT', 11, 1
    m = 10
    logger.debug("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
    kmers = seq_to_kmers(seq, k)
    bigsi = Graph.create(m=m, k=k, h=h, force=True)
    assert bigsi.kmer_size == k
    bloom = bigsi.bloom(kmers)
    bigsi.build([bloom], [sample])
    os.chmod(bigsi.graph_filename, S_IREAD | S_IRGRP | S_IROTH)
    # Can write to a read only DB
    bigsi = Graph(mode="r")
    with pytest.raises(bsddb3.db.DBAccessError):
        bigsi.insert(bloom, "1234")
    assert sample in bigsi.search(seq)
    assert bigsi.search(seq).get(sample).get('percent_kmers_found') == 100
    os.chmod(bigsi.graph_filename, S_IWUSR | S_IREAD)
    bigsi.delete_all()


import copy


def combine_samples(samples1, samples2):
    combined_samples = copy.copy(samples1)
    for x in samples2:
        if x in combined_samples:
            z = x+'_duplicate_in_merge'
        else:
            z = x
        combined_samples.append(z)
    return combined_samples


# @given(kmers1=st.lists(ST_KMER, min_size=1, max_size=9), kmers2=st.lists(ST_KMER, min_size=1, max_size=9))
# @settings(max_examples=1)
# def test_merge(kmers1, kmers2):
def test_merge():
    kmers1 = ['AAAAAAAAA']*3
    kmers2 = ['AAAAAAAAT']*9
    bigsi1 = BIGSI.create(db="./db-bigsi1/", m=10,
                          k=9, h=1, force=True)
    blooms1 = []
    for s in kmers1:
        blooms1.append(bigsi1.bloom([s]))
    samples1 = [str(i) for i in range(len(kmers1))]
    bigsi1.build(blooms1, samples1)

    bigsi2 = BIGSI.create(db="./db-bigsi2/", m=10,
                          k=9, h=1, force=True)
    blooms2 = []
    for s in kmers2:
        blooms2.append(bigsi2.bloom([s]))
    samples2 = [str(i) for i in range(len(kmers2))]
    bigsi2.build(blooms2, samples2)

    combined_samples = combine_samples(samples1, samples2)
    bigsicombined = BIGSI.create(
        db="./db-bigsi-c/", m=10, k=9, h=1, force=True)
    bigsicombined = BIGSI(db="./db-bigsi-c/", mode="c")
    bigsicombined.build(blooms1+blooms2, combined_samples)

    bigsi1.merge(bigsi2)
    bigsi1 = BIGSI(db="./db-bigsi1/")
    for i in range(10):
        assert bigsi1.graph[i] == bigsicombined.graph[i]
    for k, v in bigsicombined.metadata.items():
        assert bigsi1.metadata[k] == v
    bigsi1.delete_all()
    bigsi2.delete_all()
    bigsicombined.delete_all()

# @given(Graph=ST_GRAPH, sample=ST_SAMPLE_NAME, seq=ST_SEQ)
# @example(Graph=BIGSI, sample='0', seq='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA')


def test_insert_lookup_kmers():
    Graph, sample, seq = BIGSI, '0', 'AAAAAAAAAAAATCAAAAAAAAAAAAAAAAA'
    m, h, k = 10, 2, 31

    logger.debug("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
    kmers = list(seq_to_kmers(seq, k))
    bigsi = Graph.create(m=m, k=k, h=h, force=True)
    bloom = bigsi.bloom(kmers)
    bigsi.build([bloom], [sample])
    for kmer in kmers:
        # assert sample not in bigsi.lookup(kmer+"T")[kmer+"T"]
        ba = bitarray()
        ba.frombytes(bigsi.lookup_raw(kmer))
        assert ba[0] == True
        assert sample in bigsi.lookup(kmer)[kmer]
    assert [sample] in bigsi.lookup(kmers).values()
    bigsi.delete_all()


# TODO update for insert to take bloomfilter
# @given(Graph=ST_GRAPH, kmer=ST_KMER)
# @example(Graph=BIGSI, kmer='AAAAAAAAA')
# def test_insert_get_kmer(Graph, kmer):
def test_insert_get_kmer():
    Graph, kmer = BIGSI, 'AAAAAAAAA'
    bigsi = Graph.create(m=10, force=True)
    bloom = bigsi.bloom([kmer])
    bigsi.build([bloom], ['1'])
    assert bigsi.colours(kmer)[kmer] == [0]
    bigsi.insert(bloom, "2")
    assert bigsi.colours(kmer)[kmer] == [0, 1]
    bigsi.delete_all()


# @given(Graph=ST_GRAPH, kmer=ST_KMER)
# def test_query_kmer(Graph, kmer):
def test_query_kmer():
    Graph, kmer = BIGSI, 'AAAAAAAAA'
    bigsi = Graph.create(m=100, force=True)
    bloom1 = bigsi.bloom([kmer])
    bigsi.build([bloom1], ['1234'])
    assert bigsi.lookup(kmer) == {kmer: ['1234']}
    bigsi.insert(bloom1, '1235')
    assert bigsi.lookup(kmer) == {kmer: ['1234', '1235']}
    bigsi.delete_all()


@given(Graph=ST_GRAPH,
       s=ST_SAMPLE_NAME,
       key=st.text(min_size=1),
       value=st.text(min_size=1),
       value2=st.one_of(
           st.text(min_size=1),
           st.dictionaries(keys=st.text(min_size=1),
                           values=st.text(min_size=1)),
           st.lists(st.integers()),
           st.sets(st.integers()),
       ))
@settings(max_examples=2)
def test_add_metadata(Graph, s, key, value, value2):
    kmer = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
    bigsi = Graph.create(m=100, force=True)
    bloom1 = bigsi.bloom([kmer])
    bigsi.build([bloom1], [s])
    bigsi.add_sample_metadata(s, key, value)
    assert bigsi.lookup_sample_metadata(s).get(key) == value
    with pytest.raises(ValueError):
        bigsi.add_sample_metadata(s, key, value2)
    assert bigsi.lookup_sample_metadata(s).get(key) == value
    # Key already exists
    bigsi.add_sample_metadata(s, key, value2, overwrite=True)
    assert bigsi.lookup_sample_metadata(s).get(key) == value2

    bigsi.delete_all()


# @given(Graph=ST_GRAPH, k1=ST_KMER, k2=ST_KMER, k3=ST_KMER, k4=ST_KMER, k5=ST_KMER,
#        score=st.sampled_from([True, False]))
# @example(Graph=BIGSI, x=['AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAT', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAATA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG'], score=False)
def test_query_kmers():
    Graph = BIGSI
    x = ['AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAT',
         'AAAAAAAAAAAAAAAAAAAAAAAAAAAAATA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC',
         'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG']
    score = False
    m = 100
    h = 2
    k = 9
    logger.debug("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
    logger.debug("Testing graph kmers %s" % ",".join(x))
    k1, k2, k3, k4, k5 = x
    bigsi = Graph.create(m=m, k=k, h=h, force=True)

    bloom1 = bigsi.bloom([k1, k2, k5])
    bloom2 = bigsi.bloom([k1, k3, k5])
    bloom3 = bigsi.bloom([k4, k3, k5])

    bigsi.build([bloom1, bloom2, bloom3], ['1234', '1235', '1236'])

    assert bigsi.get_num_colours() == 3
    bigsi.num_colours = bigsi.get_num_colours()
    logger.debug("Searching graph score %s" % str(score))

    assert bigsi._search([k1, k2], threshold=0.5, score=score).get(
        '1234').get("percent_kmers_found") >= 100
    assert bigsi._search([k1, k2], threshold=0.5, score=score).get(
        '1235').get("percent_kmers_found") >= 50

    assert bigsi._search([k1, k2], score=score).get(
        '1234').get("percent_kmers_found") >= 100

    assert bigsi._search([k1, k3], threshold=0.5, score=score).get(
        '1234').get("percent_kmers_found") >= 50
    assert bigsi._search([k1, k3], threshold=0.5, score=score).get(
        '1235').get("percent_kmers_found") >= 100
    assert bigsi._search([k1, k3], threshold=0.5, score=score).get(
        '1236').get("percent_kmers_found") >= 50

    assert bigsi._search([k5], score=score).get(
        '1234').get("percent_kmers_found") >= 100
    assert bigsi._search([k5], score=score).get(
        '1235').get("percent_kmers_found") >= 100
    assert bigsi._search([k5], score=score).get(
        '1236').get("percent_kmers_found") >= 100

    bigsi.delete_all()
