from atlasseq.sketch import HyperLogLogJaccardIndex
from hypothesis import given
from hypothesis import example
import hypothesis.strategies as st

KMER = st.text(min_size=31, max_size=31, alphabet=['A', 'T', 'C', 'G'])


@given(kmers=st.lists(KMER, min_size=20, max_size=20, unique=True))
def test_jaccard_index(kmers):
    mc = HyperLogLogJaccardIndex()
    mc.delete_all()
    mc.insert(kmers, '1234')
    mc.insert(kmers, '1235')
    assert mc.jaccard_index('1234', '1235') == 1


@given(kmers1=st.lists(KMER, min_size=10, max_size=10, unique=True),
       kmers2=st.lists(KMER, min_size=10, max_size=10, unique=True))
def test_jaccard_index2(kmers1, kmers2):
    mc = HyperLogLogJaccardIndex()
    mc.delete_all()
    mc.insert(kmers1, '1234')
    mc.insert(kmers2, '1235')
    skmers1 = set(kmers1)
    skmers2 = set(kmers2)
    true_sim = float(len(skmers1 & skmers2)) / float(len(skmers1 | skmers2))
    true_sdiff = float(len(skmers1 ^ skmers2))
    true_diff = float(len(skmers1 - skmers2))

    ji = mc.jaccard_index('1234', '1235')
    sd = mc.symmetric_difference('1234', '1235')
    dd = mc.difference('1234', '1235')
    print(ji, true_sim, float(abs(ji-true_sim)))
    print(sd, true_sdiff, float(abs(sd-true_sdiff)))
    print(dd, true_diff, float(abs(dd - true_diff)))
    assert float(abs(ji-true_sim)) <= 0.2
    assert float(abs(sd-true_sdiff)) <= 5
    assert float(abs(dd - true_diff)) <= 5
