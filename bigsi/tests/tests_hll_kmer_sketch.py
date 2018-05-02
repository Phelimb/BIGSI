from bigsi.sketch import HyperLogLogJaccardIndex
from bigsi.sketch import MinHashHashSet
from hypothesis import given
from hypothesis import example
import hypothesis.strategies as st

import os

from bigsi.tests.base import ST_KMER
from bigsi.tests.base import ST_SEQ
from bigsi.tests.base import REDIS_HOST
from bigsi.tests.base import REDIS_PORT
from bigsi.utils import seq_to_kmers

SKETCHS = [HyperLogLogJaccardIndex(host=REDIS_HOST, port=REDIS_PORT),
           MinHashHashSet(host=REDIS_HOST, port=REDIS_PORT, sketch_size=100)]


@given(mc=st.sampled_from(SKETCHS), kmers=st.text(
    min_size=1000, max_size=1000, alphabet=['A', 'T', 'C', 'G']))
def test_jaccard_index1(mc, kmers):
    kmers = list(seq_to_kmers(kmers))
    mc.delete_all()
    mc.insert(kmers, '1234')
    mc.insert(kmers, '1235')
    assert mc.jaccard_index('1234', '1235') == 1


@given(mc=st.sampled_from(SKETCHS),
       kmers1=ST_SEQ,
       kmers2=ST_SEQ)
def test_jaccard_index2(mc, kmers1, kmers2):
    kmers1 = list(seq_to_kmers(kmers1))
    kmers2 = list(seq_to_kmers(kmers2))
    mc.delete_all()
    mc.insert(kmers1, '1234')
    mc.insert(kmers2, '1235')
    skmers1 = set(kmers1)
    skmers2 = set(kmers2)
    true_sim = float(len(skmers1 & skmers2)) / float(len(skmers1 | skmers2))

    ji = mc.jaccard_index('1234', '1235')
    assert float(abs(ji-true_sim)) <= 0.2


@given(kmers1=ST_SEQ),
    kmers2 = ST_SEQ)
def test_jaccard_index3(kmers1, kmers2):
    kmers1=list(seq_to_kmers(kmers1))
    kmers2=list(seq_to_kmers(kmers2))
    mc=HyperLogLogJaccardIndex(host = REDIS_HOST, port = REDIS_PORT)
    mc.delete_all()
    mc.insert(kmers1, '1234')
    mc.insert(kmers2, '1235')
    skmers1=set(kmers1)
    skmers2=set(kmers2)
    true_sim=float(len(skmers1 & skmers2)) / float(len(skmers1 | skmers2))
    true_sdiff=float(len(skmers1 ^ skmers2))
    true_diff=float(len(skmers1 - skmers2))

    ji=mc.jaccard_index('1234', '1235')
    sd=mc.symmetric_difference('1234', '1235')
    dd=mc.difference('1234', '1235')

    assert float(abs(ji-true_sim)) <= 0.2
    assert float(abs(sd-true_sdiff)) <= 5
    assert float(abs(dd - true_diff)) <= 5
