from atlasseq.utils import kmer_to_bits
from atlasseq.utils import bits_to_kmer
from atlasseq.utils import bits


def test_kmer_to_bits():
    kmer_to_bits('A') == '00'
    kmer_to_bits('C') == '01'
    kmer_to_bits('G') == '10'
    kmer_to_bits('T') == '11'
    kmer_to_bits('AT') == '0011'


def test_bits_to_kmer():
    bits_to_kmer('0011', 2) == 'AT'
    bits_to_kmer('0011', 1) == 'A'


def test_redis_bitstring_to_bitstring():
    assert bits(b'@') == [0, 1, 0, 0, 0, 0, 0, 0]
