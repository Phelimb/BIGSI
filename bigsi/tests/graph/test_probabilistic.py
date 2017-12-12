"""Tests that are unique to Probabilistic Graphs"""
from bigsi import BIGSI as Graph
from bigsi.tests.base import ST_KMER
from bigsi.tests.base import ST_SEQ
from bigsi.tests.base import ST_SAMPLE_NAME
from bigsi.tests.base import ST_GRAPH
import random
from bigsi.utils import make_hash
from bigsi.utils import reverse_comp
from hypothesis import given
from hypothesis import example
import hypothesis.strategies as st
from bigsi.bytearray import ByteArray
import tempfile
import os
import redis
from bigsi.utils import seq_to_kmers
from bigsi import BIGSI

# Add test for insert, lookup.


@given(sample=ST_SAMPLE_NAME, seq=ST_SEQ)
def test_get_bloomfilter(sample, seq):
    kmers = seq_to_kmers(seq, 31)
    bigsi = BIGSI.create(m=100, force=True)
    bigsi.insert(bigsi.bloom(kmers), sample)
    bf = bigsi.get_bloom_filter(sample)
    assert bf.length() == bigsi.graph.bloomfilter.size
    bigsi.delete_all()
