"""Tests that are common to graphs"""
import random
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
from bigsi.graph import BIGSI

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
def test_add_sample_metadata(Graph, sample):
    bigsi = Graph.create(m=100, force=True)
    assert os.path.isdir("db-bigsi")
    colour = bigsi._add_sample(sample)
    assert bigsi.sample_to_colour(sample) == colour
    assert bigsi.colour_to_sample(colour) == sample
    # assert bigsi.metadata_hgetall("colour0") == sample
    # assert bigsi.metadata_hget("colours", colour) == sample
    bigsi.delete_all()


@given(Graph=ST_GRAPH, sample=ST_SAMPLE_NAME, seq=ST_SEQ, k=ST_KMER_SIZE, m=ST_BLOOM_FILTER_SIZE, h=ST_NUM_HASHES)
# @example(Graph=BIGSI, sample='0', seq='AATTTTTATTTTTTTTTTTTTAATTAATATT', k=11, m=100, h=1)
def test_insert_and_unique_sample_names(Graph, sample, seq, k, m, h):
    logger.info("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
    kmers = seq_to_kmers(seq, k)
    m = 100
    bigsi = Graph.create(m=m, k=k, h=h, force=True)
    assert bigsi.kmer_size == k
    bloom = bigsi.bloom(kmers)
    assert len(bloom) == m
    bigsi.insert(bloom, sample)
    with pytest.raises(ValueError):
        bigsi.insert(bloom, sample)
    assert sample in bigsi.search(seq)
    assert bigsi.search(seq).get(sample).get('percent_kmers_found') == 100
    bigsi.delete_all()

from bitarray import bitarray


@given(Graph=ST_GRAPH, sample=ST_SAMPLE_NAME, seq=ST_SEQ,  k=ST_KMER_SIZE, m=ST_BLOOM_FILTER_SIZE, h=ST_NUM_HASHES)
# @example(Graph=BIGSI, sample='0', seq='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', k=11, m=10, h=10)
def test_insert_lookup_kmers(Graph, sample, seq, k, m, h):
    logger.info("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
    kmers = list(seq_to_kmers(seq, k))
    bigsi = Graph.create(m=m, k=k, h=h, force=True)
    bloom = bigsi.bloom(kmers)
    bigsi.insert(bloom, sample)
    for kmer in kmers:
        # assert sample not in bigsi.lookup(kmer+"T")[kmer+"T"]
        ba = bitarray()
        ba.frombytes(bigsi.lookup_raw(kmer))
        assert ba[0] == True
        assert sample in bigsi.lookup(kmer)[kmer]
    assert [sample] in bigsi.lookup(kmers).values()
    bigsi.delete_all()


# TODO update for insert to take bloomfilter
@given(Graph=ST_GRAPH, kmer=ST_KMER)
# @example(Graph=BIGSI, kmer='ATAAAAAAAAAAAAAAAAAAAAAAAAAAATT')
def test_insert_get_kmer(Graph, kmer):
    bigsi = Graph.create(m=100, force=True)
    bloom = bigsi.bloom([kmer])
    bigsi.insert(bloom, "1")  # insert
    assert bigsi.colours(kmer)[kmer] == [0]
    bigsi.insert(bloom, "2")
    assert bigsi.colours(kmer)[kmer] == [0, 1]
    bigsi.delete_all()


@given(Graph=ST_GRAPH, kmer=ST_KMER)
def test_query_kmer(Graph, kmer):
    bigsi = Graph.create(m=100, force=True)
    bloom1 = bigsi.bloom([kmer])
    bigsi.insert(bloom1, '1234')
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
def test_add_metadata(Graph, s, key, value, value2):
    kmer = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
    bigsi = Graph.create(m=100, force=True)
    bloom1 = bigsi.bloom([kmer])
    bigsi.insert(bloom1, s)
    bigsi.add_sample_metadata(s, key, value)
    assert bigsi.lookup_sample_metadata(s).get(key) == value
    with pytest.raises(ValueError):
        bigsi.add_sample_metadata(s, key, value2)
    assert bigsi.lookup_sample_metadata(s).get(key) == value
    # Key already exists
    bigsi.add_sample_metadata(s, key, value2, overwrite=True)
    assert bigsi.lookup_sample_metadata(s).get(key) == value2

    bigsi.delete_all()


@given(Graph=ST_GRAPH, x=st.lists(ST_KMER, min_size=5, max_size=5, unique=True),
       score=st.sampled_from([True, False]),
       k=ST_KMER_SIZE, m=ST_BLOOM_FILTER_SIZE, h=ST_NUM_HASHES)
def test_query_kmers(Graph, x, score, k, m, h):
    logger.info("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
    logger.info("Testing graph kmers %s" % ",".join(x))
    k1, k2, k3, k4, k5 = x
    bigsi = Graph.create(m=m, k=k, h=h, force=True)

    bloom1 = bigsi.bloom([k1, k2, k5])
    bloom2 = bigsi.bloom([k1, k3, k5])
    bloom3 = bigsi.bloom([k4, k3, k5])

    bigsi.build([bloom1, bloom2, bloom3], ['1234', '1235', '1236'])

    assert bigsi.get_num_colours() == 3
    bigsi.num_colours = bigsi.get_num_colours()
    logger.info("Searching graph score %s" % str(score))

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
