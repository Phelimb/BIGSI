"""Tests that are common to graphs"""
from cbg.tests.base import ST_GRAPH
from hypothesis import given
from cbg.variants import CBGVariantSearch
import hypothesis.strategies as st

import logging
logging.basicConfig()

logger = logging.getLogger(__name__)
from cbg.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)


@given(Graph=ST_GRAPH, kmer_size=st.integers(min_value=11, max_value=31))
def test_search_for_variant(Graph, kmer_size):
    cbg = Graph.create(m=100, k=kmer_size, force=True)
    variant_search = CBGVariantSearch(cbg)
    assert variant_search is not None

    assert len(variant_search.create_variant_probe_set(
        "T1C", "cbg/tests/data/ref.fasta").refs[0]) == (2*kmer_size+1)
