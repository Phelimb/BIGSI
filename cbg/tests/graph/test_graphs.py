"""Tests that are common to graphs"""
import random
from cbg.tests.base import ST_KMER
from cbg.tests.base import ST_SEQ
from cbg.tests.base import ST_SAMPLE_NAME
from cbg.tests.base import ST_GRAPH
from cbg.tests.base import ST_STORAGE
from cbg.tests.base import ST_BINARY_KMERS
from cbg.tests.base import ST_PERSISTANT_STORAGE
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


@given(Graph=ST_GRAPH, binary_kmers=ST_BINARY_KMERS)
def test_init(Graph, binary_kmers):
    mc = Graph(binary_kmers=binary_kmers, bloom_filter_size=100)
    mc.delete_all()
    assert mc.binary_kmers == binary_kmers
    assert mc.num_hashes > 0
    mc.delete_all()


@given(Graph=ST_GRAPH, store=ST_STORAGE, sample=ST_SAMPLE_NAME)
def test_add_sample_metadata(Graph, store, sample):
    mc = Graph(storage=store, bloom_filter_size=100)
    mc.delete_all()
    colour = mc._add_sample(sample)
    assert mc.get_colour_from_sample(sample) == colour
    assert mc.get_sample_from_colour(colour) == sample
    assert mc.colours_to_sample_dict().get(colour) == sample
    mc.delete_all()


@given(Graph=ST_GRAPH, store=ST_STORAGE, sample=ST_SAMPLE_NAME,
       seq=ST_SEQ)
def test_unique_sample_names(Graph, store, sample, seq):
    kmers = list(seq_to_kmers(seq))
    mc = Graph(storage=store, bloom_filter_size=100)
    mc.delete_all()
    mc.insert(kmers, sample)
    with pytest.raises(ValueError):
        mc.insert(kmers, sample)


@given(Graph=ST_GRAPH, store=ST_PERSISTANT_STORAGE, sample=ST_SAMPLE_NAME,
       seq=ST_SEQ)
def test_unique_sample_names2(Graph, store, sample, seq):
    kmers = list(seq_to_kmers(seq))
    # Persistant stores should be able to create a new instance but retain
    # metadata
    mc = Graph(storage=store, bloom_filter_size=100)
    mc.delete_all()
    mc.insert(kmers, sample)

    mc2 = Graph(storage=store, bloom_filter_size=100)
    with pytest.raises(ValueError):
        mc2.insert(kmers, sample)
    mc.delete_all()


@given(Graph=ST_GRAPH, store=ST_STORAGE, sample=ST_SAMPLE_NAME,
       seq=ST_SEQ, binary_kmers=ST_BINARY_KMERS)
def test_insert_lookup_kmers(Graph, store, sample, seq, binary_kmers):
    kmers = list(seq_to_kmers(seq))

    mc = Graph(
        binary_kmers=binary_kmers, storage=store, bloom_filter_size=100)
    mc.delete_all()
    mc.insert(kmers, sample)
    for kmer in kmers:
        assert sample in mc.lookup(kmer)[kmer]
        assert sample not in mc.lookup(kmer+"T")[kmer+"T"]
    assert [sample] in mc.lookup(kmers).values()
    mc.delete_all()


@given(Graph=ST_GRAPH, store=ST_STORAGE, kmer=ST_KMER, binary_kmers=ST_BINARY_KMERS)
def test_insert_get_kmer(Graph, store, kmer, binary_kmers):
    mc = Graph(
        binary_kmers=binary_kmers, storage=store, bloom_filter_size=100)
    mc.delete_all()
    mc.insert(kmer, "1")
    assert [v for v in mc._get_kmer_colours(kmer).values()] == [[0]]
    mc.insert(kmer, "2")
    assert [v for v in mc._get_kmer_colours(kmer).values()] == [[0, 1]]
    mc.delete_all()


@given(Graph=ST_GRAPH, kmer=ST_KMER, store=ST_STORAGE, binary_kmers=ST_BINARY_KMERS)
def test_query_kmer(Graph, kmer, store, binary_kmers):
    mc = Graph(
        binary_kmers=binary_kmers, storage=store, bloom_filter_size=100)
    mc.delete_all()
    bloom1 = mc.bloom([kmer])

    c = mc._add_sample('1234')
    mc.graph.insert(bloom1, c)
    assert mc.lookup(kmer) == {kmer: ['1234']}
    c = mc._add_sample('1235')
    mc.graph.insert(bloom1, c)

    assert mc.lookup(kmer) == {kmer: ['1234', '1235']}
    mc.delete_all()


@given(Graph=ST_GRAPH, x=st.lists(ST_KMER, min_size=5, max_size=5, unique=True),
       store=ST_STORAGE, binary_kmers=ST_BINARY_KMERS)
def test_query_kmers(Graph, x, store, binary_kmers):
    k1, k2, k3, k4, k5 = x
    mc = Graph(
        binary_kmers=binary_kmers, storage=store, bloom_filter_size=1000)
    mc.delete_all()

    bloom1 = mc.bloom([k1, k2])
    bloom2 = mc.bloom([k1, k3])
    bloom3 = mc.bloom([k4, k3])

    mc.build([bloom1, bloom2, bloom3], ['1234', '1235', '1236'])

    assert mc.get_num_colours() == 3
    mc.num_colours = mc.get_num_colours()
    assert mc._search([k1, k2], threshold=0.5) == {
        '1234': 1, '1235': 0.5}
    assert mc._search([k1, k2]) == {
        '1234': 1}
    assert mc._search([k1, k3], threshold=0.5) == {
        '1234': 0.5, '1235': 1, '1236': 0.5}
    mc.delete_all()


# @given(Graph=ST_GRAPH, x=st.lists(ST_KMER, min_size=5, max_size=5, unique=True),
#        store=ST_STORAGE, binary_kmers=ST_BINARY_KMERS)
# def test_query_kmers2(Graph, x, store, binary_kmers):
#     k1, k2, k3, k4, k5 = x
#     mc = Graph(
#         binary_kmers=binary_kmers, storage=store, bloom_filter_size=100)
#     mc.delete_all()

#     bloom1 = mc.bloom([k1, k2])
#     bloom2 = mc.bloom([k1, k3])
#     bloom3 = mc.bloom([k4, k3])
#     mc.build([bloom1, bloom2, bloom3], ['1234', '1235', '1236'])

#     assert mc.get_num_colours() == 3
#     mc.num_colours = mc.get_num_colours()
#     assert mc._search_kmers_threshold_1(
#         [k1, k2]) == mc._search_kmers_threshold_not_1([k1, k2], threshold=1)
#     assert mc._search_kmers_threshold_1(
#         [k1, k3]) == mc._search_kmers_threshold_not_1([k1, k3], threshold=1)
#     mc.delete_all()
