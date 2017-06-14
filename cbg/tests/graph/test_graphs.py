"""Tests that are common to graphs"""
import random
from cbg.tests.base import ST_KMER
from cbg.tests.base import ST_SEQ
from cbg.tests.base import ST_SAMPLE_NAME
from cbg.tests.base import ST_GRAPH
from cbg.tests.base import ST_KMER_SIZE
from cbg.tests.base import ST_BLOOM_FILTER_SIZE
from cbg.tests.base import ST_NUM_HASHES
from cbg.utils import make_hash
from cbg.utils import reverse_comp
from hypothesis import given
from hypothesis import example
from hypothesis import settings
import hypothesis.strategies as st
from cbg.bytearray import ByteArray
import os
import pytest
from cbg.utils import seq_to_kmers
from cbg.graph import CBG

import logging
logging.basicConfig()

logger = logging.getLogger(__name__)
from cbg.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)


@given(Graph=ST_GRAPH)
def test_create(Graph):
    cbg = Graph.create(m=100, force=True)
    assert cbg.kmer_size == 31
    assert os.path.isdir("db-cbg")
    cbg.delete_all()


@given(Graph=ST_GRAPH)
def test_force_create(Graph):
    cbg = Graph.create(force=True)
    with pytest.raises(FileExistsError):
        Graph.create(m=100, force=False)
    cbg = Graph.create(force=True)
    assert cbg.kmer_size == 31
    assert os.path.isdir("db-cbg")
    cbg.delete_all()


@given(Graph=ST_GRAPH, sample=ST_SAMPLE_NAME)
def test_add_sample_metadata(Graph, sample):
    cbg = Graph.create(m=100, force=True)
    assert os.path.isdir("db-cbg")
    colour = cbg._add_sample(sample)
    assert cbg.sample_to_colour(sample) == colour
    assert cbg.colour_to_sample(colour) == sample
    assert cbg.metadata_hgetall("colour0") == sample
    # assert cbg.metadata_hget("colours", colour) == sample
    cbg.delete_all()


@given(Graph=ST_GRAPH, sample=ST_SAMPLE_NAME, seq=ST_SEQ, k=ST_KMER_SIZE, m=ST_BLOOM_FILTER_SIZE, h=ST_NUM_HASHES)
# @example(Graph=CBG, sample='0', seq='AATTTTTATTTTTTTTTTTTTAATTAATATT', k=11, m=100, h=1)
def test_insert_and_unique_sample_names(Graph, sample, seq, k, m, h):
    logger.info("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
    kmers = seq_to_kmers(seq, k)
    m = 100
    cbg = Graph.create(m=m, k=k, h=h, force=True)
    assert cbg.kmer_size == k
    bloom = cbg.bloom(kmers)
    assert len(bloom) == m
    cbg.insert(bloom, sample)
    with pytest.raises(ValueError):
        cbg.insert(bloom, sample)
    assert sample in cbg.search(seq)
    assert cbg.search(seq).get(sample).get('percent_kmers_found') == 100
    cbg.delete_all()

from bitarray import bitarray


@given(Graph=ST_GRAPH, sample=ST_SAMPLE_NAME, seq=ST_SEQ,  k=ST_KMER_SIZE, m=ST_BLOOM_FILTER_SIZE, h=ST_NUM_HASHES)
# @example(Graph=CBG, sample='0', seq='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', k=11, m=10, h=10)
def test_insert_lookup_kmers(Graph, sample, seq, k, m, h):
    logger.info("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
    kmers = list(seq_to_kmers(seq, k))
    cbg = Graph.create(m=m, k=k, h=h, force=True)
    bloom = cbg.bloom(kmers)
    cbg.insert(bloom, sample)
    for kmer in kmers:
        # assert sample not in cbg.lookup(kmer+"T")[kmer+"T"]
        ba = bitarray()
        ba.frombytes(cbg.lookup_raw(kmer))
        assert ba[0] == True
        assert sample in cbg.lookup(kmer)[kmer]
    assert [sample] in cbg.lookup(kmers).values()
    cbg.delete_all()


# TODO update for insert to take bloomfilter
@given(Graph=ST_GRAPH, kmer=ST_KMER)
# @example(Graph=CBG, kmer='ATAAAAAAAAAAAAAAAAAAAAAAAAAAATT')
def test_insert_get_kmer(Graph, kmer):
    cbg = Graph.create(m=100, force=True)
    bloom = cbg.bloom([kmer])
    cbg.insert(bloom, "1")  # insert
    assert cbg.colours(kmer)[kmer] == [0]
    cbg.insert(bloom, "2")
    assert cbg.colours(kmer)[kmer] == [0, 1]
    cbg.delete_all()


@given(Graph=ST_GRAPH, kmer=ST_KMER)
def test_query_kmer(Graph, kmer):
    cbg = Graph.create(m=100, force=True)
    bloom1 = cbg.bloom([kmer])
    cbg.insert(bloom1, '1234')
    assert cbg.lookup(kmer) == {kmer: ['1234']}
    cbg.insert(bloom1, '1235')
    assert cbg.lookup(kmer) == {kmer: ['1234', '1235']}
    cbg.delete_all()


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
    cbg = Graph.create(m=100, force=True)
    bloom1 = cbg.bloom([kmer])
    cbg.insert(bloom1, s)
    cbg.add_sample_metadata(s, key, value)
    assert cbg.lookup_sample_metadata(s).get(key) == value
    with pytest.raises(ValueError):
        cbg.add_sample_metadata(s, key, value2)
    assert cbg.lookup_sample_metadata(s).get(key) == value
    # Key already exists
    cbg.add_sample_metadata(s, key, value2, overwrite=True)
    assert cbg.lookup_sample_metadata(s).get(key) == value2

    cbg.delete_all()


@given(Graph=ST_GRAPH, x=st.lists(ST_KMER, min_size=5, max_size=5, unique=True),
       score=st.sampled_from([True, False]),
       k=ST_KMER_SIZE, m=ST_BLOOM_FILTER_SIZE, h=ST_NUM_HASHES)
def test_query_kmers(Graph, x, score, k, m, h):
    logger.info("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
    logger.info("Testing graph kmers %s" % ",".join(x))
    k1, k2, k3, k4, k5 = x
    cbg = Graph.create(m=m, k=k, h=h, force=True)

    bloom1 = cbg.bloom([k1, k2, k5])
    bloom2 = cbg.bloom([k1, k3, k5])
    bloom3 = cbg.bloom([k4, k3, k5])

    cbg.build([bloom1, bloom2, bloom3], ['1234', '1235', '1236'])

    assert cbg.get_num_colours() == 3
    cbg.num_colours = cbg.get_num_colours()
    logger.info("Searching graph score %s" % str(score))

    assert cbg._search([k1, k2], threshold=0.5, score=score).get(
        '1234').get("percent_kmers_found") >= 100
    assert cbg._search([k1, k2], threshold=0.5, score=score).get(
        '1235').get("percent_kmers_found") >= 50

    assert cbg._search([k1, k2], score=score).get(
        '1234').get("percent_kmers_found") >= 100

    assert cbg._search([k1, k3], threshold=0.5, score=score).get(
        '1234').get("percent_kmers_found") >= 50
    assert cbg._search([k1, k3], threshold=0.5, score=score).get(
        '1235').get("percent_kmers_found") >= 100
    assert cbg._search([k1, k3], threshold=0.5, score=score).get(
        '1236').get("percent_kmers_found") >= 50

    assert cbg._search([k5], score=score).get(
        '1234').get("percent_kmers_found") >= 100
    assert cbg._search([k5], score=score).get(
        '1235').get("percent_kmers_found") >= 100
    assert cbg._search([k5], score=score).get(
        '1236').get("percent_kmers_found") >= 100

    cbg.delete_all()
