"""Tests that are unique to Probabilistic Graphs"""
from cbg import CBG as Graph
from cbg.tests.base import ST_KMER
from cbg.tests.base import ST_SEQ
from cbg.tests.base import ST_SAMPLE_NAME
from cbg.tests.base import ST_GRAPH
import random
from cbg.utils import make_hash
from cbg.utils import reverse_comp
from hypothesis import given
from hypothesis import example
import hypothesis.strategies as st
from cbg.bytearray import ByteArray
import tempfile
import os
import redis
from cbg.utils import seq_to_kmers
from cbg import CBG

# Add test for insert, lookup.


@given(sample=ST_SAMPLE_NAME, seq=ST_SEQ)
def test_get_bloomfilter(sample, seq):
    kmers = seq_to_kmers(seq, 31)
    cbg = CBG.create(m=100, force=True)
    cbg.insert(cbg.bloom(kmers), sample)
    bf = cbg.get_bloom_filter(sample)
    assert bf.length() == cbg.graph.bloomfilter.size
    cbg.delete_all()
