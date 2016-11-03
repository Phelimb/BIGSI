from atlasseq.sketch import HyperLogLogJaccardIndex
from hypothesis import given
from hypothesis import example
import hypothesis.strategies as st

KMER = st.text(min_size=31, max_size=31, alphabet=['A', 'T', 'C', 'G'])


@given(kmers=st.lists(KMER, min_size=100, max_size=100, unique=True))
def test_jaccard_index(kmers):
    mc = HyperLogLogJaccardIndex()
    mc.delete_all()
    mc.insert(kmers, '1234')
    mc.insert(kmers, '1235')
    assert mc.jaccard_index('1234', '1235') == 1


@given(kmers1=st.lists(KMER, min_size=100, max_size=100, unique=True),
       kmers2=st.lists(KMER, min_size=100, max_size=100, unique=True))
def test_jaccard_index2(kmers1, kmers2):
    mc = HyperLogLogJaccardIndex()
    mc.delete_all()
    mc.insert(kmers1, '1234')
    mc.insert(kmers2, '1235')
    skmers1 = set(kmers1)
    skmers2 = set(kmers2)
    true_sim = float(len(skmers1 & skmers2)) / float(len(skmers1 | skmers2))
    assert true_sim*.8 <= mc.jaccard_index(
        '1234', '1235') <= true_sim*1.2


@given(kmers1=st.lists(KMER, min_size=100, max_size=100, unique=True),
       kmers2=st.lists(KMER, min_size=100, max_size=100, unique=True))
def test_kmer_diff(kmers1, kmers2):
    mc = HyperLogLogJaccardIndex()
    mc.delete_all()
    mc.insert(kmers1, '1234')
    mc.insert(kmers2, '1235')
    skmers1 = set(kmers1)
    skmers2 = set(kmers2)
    true_diff = float(len(skmers1 ^ skmers2))
    # true_diff2 = float(len(skmers2 - skmers1))
    assert true_diff*.8 <= mc.symmetric_difference(
        '1234', '1235') <= true_diff*1.3
    true_diff = float(len(skmers1 - skmers2))

    assert true_diff*.8 <= mc.difference(
        '1234', '1235') <= true_diff*1.3
    true_diff = float(len(skmers2 - skmers1))

    assert true_diff*.8 <= mc.difference(
        '1235', '1234') <= true_diff*1.3
