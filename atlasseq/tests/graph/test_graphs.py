"""Tests that are common to graphs"""
import random
from atlasseq.tests.base import ST_KMER
from atlasseq.tests.base import ST_SAMPLE_NAME
from atlasseq.tests.base import ST_GRAPH
from atlasseq.tests.base import ST_STORAGE
from atlasseq.tests.base import ST_BINARY_KMERS
from atlasseq.tests.base import ST_PERSISTANT_STORAGE
from atlasseq.utils import make_hash
from atlasseq.utils import reverse_comp
from hypothesis import given
from hypothesis import example
import hypothesis.strategies as st
from atlasseq.bytearray import ByteArray
import os
import pytest


@given(Graph=ST_GRAPH, binary_kmers=ST_BINARY_KMERS)
def test_init(Graph, binary_kmers):
    mc = Graph(binary_kmers=binary_kmers)
    assert mc.binary_kmers == binary_kmers
    assert mc.num_hashes > 0


@given(Graph=ST_GRAPH, store=ST_STORAGE, sample=ST_SAMPLE_NAME)
def test_add_sample_metadata(Graph, store, sample):
    mc = Graph(storage=store)
    mc.delete_all()
    colour = mc._add_sample(sample)
    assert mc.get_colour_from_sample(sample) == colour
    assert mc.get_sample_from_colour(colour) == sample
    assert mc.colours_to_sample_dict().get(colour) == sample


@given(Graph=ST_GRAPH, store=ST_STORAGE, sample=ST_SAMPLE_NAME,
       kmers=st.lists(ST_KMER, min_size=1, max_size=100))
def test_unique_sample_names(Graph, store, sample, kmers):
    mc = Graph(storage=store)
    mc.delete_all()
    mc.insert(kmers, sample)
    with pytest.raises(ValueError):
        mc.insert(kmers, sample)


@given(Graph=ST_GRAPH, store=ST_PERSISTANT_STORAGE, sample=ST_SAMPLE_NAME,
       kmers=st.lists(ST_KMER, min_size=1, max_size=100))
def test_unique_sample_names2(Graph, store, sample, kmers):
    # Persistant stores should be able to create a new instance but retain
    # metadata
    mc = Graph(storage=store)
    mc.delete_all()
    mc.insert(kmers, sample)
    mc2 = Graph(storage=store)
    with pytest.raises(ValueError):
        mc2.insert(kmers, sample)


@given(Graph=ST_GRAPH, store=ST_STORAGE, sample=ST_SAMPLE_NAME,
       kmers=st.lists(ST_KMER, min_size=1, max_size=100), binary_kmers=ST_BINARY_KMERS)
def test_insert_lookup_kmers(Graph, store, sample, kmers, binary_kmers):
    mc = Graph(binary_kmers=binary_kmers, storage=store)
    mc.delete_all()
    mc.insert(kmers, sample)
    for kmer in kmers:
        assert sample in mc.lookup(kmer)[kmer]
        assert sample not in mc.lookup(kmer+"T")[kmer+"T"]
    assert [sample] in mc.lookup(kmers).values()


@given(Graph=ST_GRAPH, store=ST_STORAGE, kmer=ST_KMER, binary_kmers=ST_BINARY_KMERS)
def test_insert_get_kmer(Graph, store, kmer, binary_kmers):
    mc = Graph(binary_kmers=binary_kmers, storage=store)
    mc.delete_all()
    mc.insert(kmer, "1")
    assert [v for v in mc._get_kmer_colours(kmer).values()] == [[0]]
    mc.insert(kmer, "2")
    assert [v for v in mc._get_kmer_colours(kmer).values()] == [[0, 1]]


@given(Graph=ST_GRAPH, kmer=ST_KMER, store=ST_STORAGE, binary_kmers=ST_BINARY_KMERS)
def test_query_kmer(Graph, kmer, store, binary_kmers):
    mc = Graph(binary_kmers=binary_kmers, storage=store)
    mc.delete_all()
    mc.insert(kmer, '1234')
    assert mc.lookup(kmer) == {kmer: ['1234']}
    mc.insert(kmer, '1235')
    assert mc.lookup(kmer) == {kmer: ['1234', '1235']}


@given(Graph=ST_GRAPH, x=st.lists(ST_KMER, min_size=5, max_size=5, unique=True),
       store=ST_STORAGE, binary_kmers=ST_BINARY_KMERS)
def test_query_kmers(Graph, x, store, binary_kmers):
    k1, k2, k3, k4, k5 = x
    mc = Graph(binary_kmers=binary_kmers, storage=store)
    mc.delete_all()

    mc.insert([k1, k2], '1234')
    mc.insert([k1, k3], '1235')
    mc.insert([k4, k3], '1236')
    assert mc.get_num_colours() == 3
    mc.num_colours = mc.get_num_colours()
    assert mc._search([k1, k2], threshold=0.5) == {
        '1234': 1, '1235': 0.5}
    assert mc._search([k1, k2]) == {
        '1234': 1}
    assert mc._search([k1, k3], threshold=0.5) == {
        '1234': 0.5, '1235': 1, '1236': 0.5}


# @given(Graph=ST_GRAPH, kmers=st.lists(ST_KMER, min_size=10, max_size=10, unique=True),
#        binary_kmers=ST_BINARY_KMERS, store=ST_STORAGE)
# def test_count_kmers(Graph, kmers, binary_kmers, store):
#     mc = Graph(binary_kmers=binary_kmers, storage=store)
#     mc.delete_all()
#     mc.add_sample('1234')
#     mc._insert(kmers, 0, sample='1234')
#     mc.add_to_kmers_count(kmers, sample='1234')
#     assert 7 < mc.count_kmers(sample='1234') < 12
