from bigsi.bloom import generate_hashes


def test_generate_hashes():
    assert generate_hashes("ATT", 3, 25) == {2, 15, 17}
    assert generate_hashes("ATT", 1, 25) == {15}
    assert generate_hashes("ATT", 2, 50) == {15, 27}
