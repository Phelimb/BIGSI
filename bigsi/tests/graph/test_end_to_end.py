import pytest
import json
from bitarray import bitarray

from bigsi.tests.base import CONFIGS
from bigsi import BIGSI
from bigsi.storage import get_storage
from bigsi.utils import seq_to_kmers
import pytest


def test_create():
    for config in CONFIGS:
        get_storage(config).delete_all()
        bloomfilters = [BIGSI.bloom(config, ["ATC", "ATA"])]
        samples = ["1"]
        bigsi = BIGSI.build(config, bloomfilters, samples)
        assert bigsi.kmer_size == 3
        assert bigsi.bloomfilter_size == 1000
        assert bigsi.num_hashes == 3
        assert bigsi.num_samples == 1
        assert bigsi.lookup("ATC") == {"ATC": bitarray("1")}
        assert bigsi.colour_to_sample(0) == "1"
        assert bigsi.sample_to_colour("1") == 0
        bigsi.delete()


def test_insert():
    for config in CONFIGS:
        get_storage(config).delete_all()
        bloomfilters = [BIGSI.bloom(config, ["ATC", "ATA"])]
        samples = ["1"]
        bigsi = BIGSI.build(config, bloomfilters, samples)
        bloomfilter_2 = BIGSI.bloom(config, ["ATC", "ATT"])
        bigsi.insert(bloomfilter_2, "2")
        assert bigsi.kmer_size == 3
        assert bigsi.bloomfilter_size == 1000
        assert bigsi.num_hashes == 3
        assert bigsi.num_samples == 2
        assert bigsi.lookup(["ATC", "ATA", "ATT"]) == {
            "ATC": bitarray("11"),
            "ATA": bitarray("10"),
            "ATT": bitarray("01"),
        }
        assert bigsi.colour_to_sample(0) == "1"
        assert bigsi.sample_to_colour("1") == 0
        assert bigsi.colour_to_sample(1) == "2"
        assert bigsi.sample_to_colour("2") == 1
        bigsi.delete()


def test_unique_sample_names():

    for config in CONFIGS:
        get_storage(config).delete_all()
        bloom = BIGSI.bloom(config, ["ATC", "ATA"])
        bigsi = BIGSI.build(config, [bloom], ["1"])
        with pytest.raises(ValueError):
            bigsi.insert(bloom, "1")
        assert bigsi.num_samples == 1
        assert bigsi.lookup(["ATC", "ATA", "ATT"]) == {
            "ATC": bitarray("1"),
            "ATA": bitarray("1"),
            "ATT": bitarray("0"),
        }
        bigsi.delete()


def test_exact_search():
    config = CONFIGS[0]
    kmers_1 = seq_to_kmers("ATACACAAT", config["k"])
    kmers_2 = seq_to_kmers("ACAGAGAAC", config["k"])
    bloom1 = BIGSI.bloom(config, kmers_1)
    bloom2 = BIGSI.bloom(config, kmers_2)
    for config in CONFIGS:
        get_storage(config).delete_all()
        bigsi = BIGSI.build(config, [bloom1, bloom2], ["a", "b"])
        assert bigsi.search("ATACACAAT")[0] == {
            "percent_kmers_found": 100,
            "num_kmers": 6,
            "num_kmers_found": 6,
            "sample_name": "a",
        }
        assert bigsi.search("ACAGAGAAC")[0] == {
            "percent_kmers_found": 100,
            "num_kmers": 6,
            "num_kmers_found": 6,
            "sample_name": "b",
        }
        assert bigsi.search("ACAGTTAAC") == []
        bigsi.delete()


@pytest.mark.skip(
    reason="Passes in isolation, but fails when run with the rest of the tests"
)
def test_inexact_search():
    for config in CONFIGS:
        get_storage(config).delete_all()
    config = CONFIGS[0]
    kmers_1 = seq_to_kmers("ATACACAAT", config["k"])
    kmers_2 = seq_to_kmers("ATACACAAC", config["k"])
    bloom1 = BIGSI.bloom(config, kmers_1)
    bloom2 = BIGSI.bloom(config, kmers_2)

    for config in CONFIGS:
        get_storage(config).delete_all()
        with pytest.raises(BaseException):
            BIGSI(config)
        bigsi = BIGSI.build(config, [bloom1, bloom2], ["a", "b"])
        assert bigsi.search("ACAGTTAAC", 0.5) == []
        assert bigsi.lookup("AAT") == {"AAT": bitarray("10")}

        results = bigsi.search("ATACACAAT", 0.5)
        assert results[0] == {
            "percent_kmers_found": 100.0,
            "num_kmers": 6,
            "num_kmers_found": 6,
            "sample_name": "a",
        }
        assert (
            json.dumps(results[0])
            == '{"percent_kmers_found": 100.0, "num_kmers": 6, "num_kmers_found": 6, "sample_name": "a"}'
        )
        assert results[1] == {
            "percent_kmers_found": 83.33,
            "num_kmers": 6,
            "num_kmers_found": 5,
            "sample_name": "b",
        }
        bigsi.delete()


##
@pytest.mark.skip(reason="TODO, fix test to work on single config")
def test_merge():
    for config in CONFIGS:
        get_storage(config).delete_all()
    config = CONFIGS[0]
    kmers_1 = seq_to_kmers("ATACACAAT", config["k"])
    kmers_2 = seq_to_kmers("ATACACAAC", config["k"])
    bloom1 = BIGSI.bloom(config, kmers_1)
    bloom2 = BIGSI.bloom(config, kmers_2)

    bigsi1 = BIGSI.build(CONFIGS[0], [bloom1], ["a"])
    bigsi2 = BIGSI.build(CONFIGS[1], [bloom2], ["b"])
    bigsic = BIGSI.build(CONFIGS[2], [bloom1, bloom2], ["a", "b"])

    bigsi1.merge(bigsi2)

    assert bigsi1.search("ATACACAAT", 0.5) == bigsic.search("ATACACAAT", 0.5)
    bigsi1.delete()
    bigsi2.delete()
    bigsic.delete()
