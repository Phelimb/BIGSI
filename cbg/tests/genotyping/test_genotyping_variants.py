"""Tests that are common to graphs"""
from cbg.tests.base import ST_GRAPH
from hypothesis import given
from cbg.variants import CBGVariantSearch
from cbg.variants import CBGAminoAcidMutationSearch
import hypothesis.strategies as st
from cbg import CBG

import logging
logging.basicConfig()

logger = logging.getLogger(__name__)
from cbg.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)


@given(Graph=ST_GRAPH, kmer_size=st.integers(min_value=11, max_value=31))
def test_create_variant_probe_set(Graph, kmer_size):
    cbg = Graph.create(m=100, k=kmer_size, force=True)
    variant_search = CBGVariantSearch(cbg)
    assert variant_search is not None

    assert len(variant_search.create_variant_probe_set(
        "T1C", "cbg/tests/data/ref.fasta").refs[0]) == (2*kmer_size+1)


@given(Graph=ST_GRAPH)
def test_search_for_variant(Graph):
    kmer_size = 21
    cbg = Graph.create(m=100, k=kmer_size, force=True)
    variant_search = CBGVariantSearch(cbg, "cbg/tests/data/ref.fasta")
    # Add a the reference seq, the alternate and both as samples
    variant_probe_set = variant_search.create_variant_probe_set(
        "T1C")
    ref = variant_probe_set.refs[0]
    alt = variant_probe_set.alts[0]
    bloom1 = cbg.bloom(cbg.seq_to_kmers(ref))
    bloom2 = cbg.bloom(cbg.seq_to_kmers(alt))
    cbg.insert(bloom1, 'ref')
    cbg.insert(bloom2, 'alt')

    results = variant_search.search_for_variant("T", 1, "C")
    assert results.get("T1C").get("ref").get("genotype") == "0/0"
    assert results.get("T1C").get("alt").get("genotype") == "1/1"


def test_search_for_amino_acid_mutation():
    kmer_size = 21
    cbg = CBG.create(m=100, k=kmer_size, force=True)
    variant_search = CBGAminoAcidMutationSearch(
        cbg, "cbg/tests/data/ref.fasta", "cbg/tests/data/ref.gb")

    var_name1 = variant_search.aa2dna.get_variant_names("rpoB", "S450X", True)[
        0]
    var_name2 = variant_search.aa2dna.get_variant_names("rpoB", "S450X", True)[
        4]
    print(var_name1)
    print(var_name2)

    # # Add a the reference seq, the alternate and both as samples
    variant_probe_set1 = variant_search.create_variant_probe_set(var_name1)
    variant_probe_set2 = variant_search.create_variant_probe_set(var_name2)

    ref1 = variant_probe_set1.refs[0]
    alt1 = variant_probe_set1.alts[0]
    ref2 = variant_probe_set2.refs[0]
    alt2 = variant_probe_set2.alts[0]
    bloom1 = cbg.bloom(cbg.seq_to_kmers(ref1))
    bloom2 = cbg.bloom(cbg.seq_to_kmers(alt1))
    bloom3 = cbg.bloom(cbg.seq_to_kmers(ref2))
    bloom4 = cbg.bloom(cbg.seq_to_kmers(alt2))
    cbg.insert(bloom1, 'ref1')
    cbg.insert(bloom2, 'alt1')
    cbg.insert(bloom3, 'ref2')
    cbg.insert(bloom4, 'alt2')

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
