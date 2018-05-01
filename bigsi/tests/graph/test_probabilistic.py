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
from hypothesis import settings
import hypothesis.strategies as st
from bigsi.bytearray import ByteArray
import tempfile
import os
import redis
from bigsi.utils import seq_to_kmers
from bigsi import BIGSI

# Add test for insert, lookup.


@given(seq=ST_SEQ)
@settings(max_examples=5)
def test_get_bloomfilter(seq):
    sample = "1234"
    kmers = seq_to_kmers(seq, 31)
    bigsi = BIGSI.create(m=10, force=True)
    bigsi.build([bigsi.bloom(kmers)], [sample])
    bf = bigsi.get_bloom_filter(sample)
    assert bf.length() == bigsi.graph.bloomfilter.size
    bigsi.delete_all()
