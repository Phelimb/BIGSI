"""Tests that are common to graphs"""
from bigsi.tests.base import ST_GRAPH
from hypothesis import given
from bigsi.variants import BIGSIVariantSearch
from bigsi.variants import BIGSIAminoAcidMutationSearch
import hypothesis.strategies as st
from bigsi import BIGSI

import logging
logging.basicConfig()

logger = logging.getLogger(__name__)
from bigsi.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)
import unittest
import os
import pytest


@given(Graph=ST_GRAPH, kmer_size=st.integers(min_value=11, max_value=31))
def test_create_variant_probe_set(Graph, kmer_size):
    bigsi = Graph.create(m=1000, k=kmer_size, force=True)
    variant_search = BIGSIVariantSearch(bigsi, "bigsi/tests/data/ref.fasta")
    assert variant_search is not None

    assert len(variant_search.create_variant_probe_set(
        "T1C").refs[0]) == (2*kmer_size+1)


@given(Graph=ST_GRAPH)
def test_search_for_variant(Graph):
    kmer_size = 21
    bigsi = Graph.create(m=1000, k=kmer_size, force=True)
    variant_search = BIGSIVariantSearch(bigsi, "bigsi/tests/data/ref.fasta")
    # Add a the reference seq, the alternate and both as samples
    variant_probe_set = variant_search.create_variant_probe_set(
        "T1C")
    ref = variant_probe_set.refs[0]
    alt = variant_probe_set.alts[0]
    bloom1 = bigsi.bloom(bigsi.seq_to_kmers(ref))
    bloom2 = bigsi.bloom(bigsi.seq_to_kmers(alt))
    bigsi.build([bloom1, bloom2], ['ref', 'alt'])

    results = variant_search.search_for_variant("T", 1, "C")
    print(results)
    assert results.get("T1C").get("ref").get("genotype") == "0/0"
    assert results.get("T1C").get("alt").get("genotype") == "1/1"


@pytest.mark.skipif('"TRAVIS" in os.environ and os.environ["TRAVIS"] == "true"')
def test_search_for_amino_acid_mutation():
    kmer_size = 21
    bigsi = BIGSI.create(m=1000, k=kmer_size, force=True)
    variant_search = BIGSIAminoAcidMutationSearch(
        bigsi, "bigsi/tests/data/ref.fasta", "bigsi/tests/data/ref.gb")

    var_name1 = variant_search.aa2dna.get_variant_names("rpoB", "S450X", True)[
        0]
    var_name2 = variant_search.aa2dna.get_variant_names("rpoB", "S450X", True)[
        4]

    # # Add a the reference seq, the alternate and both as samples
    variant_probe_set1 = variant_search.create_variant_probe_set(var_name1)
    variant_probe_set2 = variant_search.create_variant_probe_set(var_name2)

    ref1 = variant_probe_set1.refs[0]
    alt1 = variant_probe_set1.alts[0]
    ref2 = variant_probe_set2.refs[0]
    alt2 = variant_probe_set2.alts[0]
    bloom1 = bigsi.bloom(bigsi.seq_to_kmers(ref1))
    bloom2 = bigsi.bloom(bigsi.seq_to_kmers(alt1))
    bloom3 = bigsi.bloom(bigsi.seq_to_kmers(ref2))
    bloom4 = bigsi.bloom(bigsi.seq_to_kmers(alt2))
    bigsi.build([bloom1, bloom2, bloom3, bloom4],
                ['ref1', 'alt1', 'ref2', 'alt2'])

    results = variant_search.search_for_amino_acid_variant(
        "rpoB", "S", 450, "X")
    assert results.get("rpoB_S450X").get("ref1").get("genotype") == "0/0"
    assert results.get("rpoB_S450X").get("ref1").get("aa_mut")[:-1] == "S450"
    assert results.get("rpoB_S450X").get(
        "ref1").get("variant")[:-3] == var_name1[:-3]

    assert results.get("rpoB_S450X").get("ref2").get("genotype") == "0/0"
    assert results.get("rpoB_S450X").get("ref2").get("aa_mut")[:-1] == "S450"
    assert results.get("rpoB_S450X").get(
        "ref2").get("variant")[:-3] == var_name2[:-3]

    assert results.get("rpoB_S450X").get("alt1").get("genotype") == "1/1"
    assert results.get("rpoB_S450X").get("alt1").get("aa_mut") == "S450K"
    assert results.get("rpoB_S450X").get("alt1").get("variant") == var_name1

    assert results.get("rpoB_S450X").get("alt2").get("genotype") == "1/1"
    assert results.get("rpoB_S450X").get("alt2").get("aa_mut") == "S450I"
    assert results.get("rpoB_S450X").get("alt2").get("variant") == var_name2
