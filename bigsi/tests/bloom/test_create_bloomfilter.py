from bigsi.bloom import generate_hashes
from bigsi.bloom import BloomFilter


def test_generate_hashes():
    assert generate_hashes("ATT", 3, 25) == {2, 15, 17}
    assert generate_hashes("ATT", 1, 25) == {15}
    assert generate_hashes("ATT", 2, 50) == {15, 27}


def test_create_bloom():
    for i in range(3):
        kmers1 = ["ATT", "ATC"]
        bloomfilter1 = BloomFilter(m=25, h=3)
        bloomfilter1.update(kmers1)

        kmers2 = ["ATT", "ATT"]
        bloomfilter2 = BloomFilter(m=25, h=3)
        bloomfilter2.update(kmers2)

        assert bloomfilter1.bitarray != bloomfilter2.bitarray
