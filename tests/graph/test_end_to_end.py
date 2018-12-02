import pytest
import json
from bitarray import bitarray

from tests.base import CONFIGS
from bigsi import BIGSI
from bigsi.storage import get_storage
from bigsi.utils import seq_to_kmers


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


# def test_row_merge():
#     primary_db = "tests/data/merge/test-bigsi-1"
#     try:
#         shutil.rmtree(primary_db)
#     except:
#         pass
#     shutil.copytree("tests/data/merge/test-bigsi-1-init", primary_db)
#     bigsi1 = BIGSI("tests/data/merge/test-bigsi-1")
#     bigsi2 = BIGSI("tests/data/merge/test-bigsi-2")
#     bigsi3 = BIGSI("tests/data/merge/test-bigsi-3")
#     assert bigsi1.graph[1] != None
#     assert bigsi2.graph[2] == None
#     assert bigsi2.graph[101] != None
#     assert bigsi3.graph[201] != None
#     assert bigsi1.graph[101] == None
#     assert bigsi1.graph[101] != bigsi2.graph[101]
#     assert bigsi1.graph[201] != bigsi3.graph[201]
#     assert bigsi1.graph[299] != bigsi3.graph[299]

#     bigsi1.row_merge([bigsi2, bigsi3])
#     assert bigsi1.graph[101] != None
#     assert bigsi1.graph[101] == bigsi2.graph[101]
#     assert bigsi1.graph[201] != bigsi2.graph[201]
#     assert bigsi1.graph[201] == bigsi3.graph[201]
#     assert bigsi1.graph[299] == bigsi3.graph[299]

#     # blooms1 = []
#     # for s in kmers1:
#     #     blooms1.append(bigsi1.bloom([s]))
#     # samples1 = [str(i) for i in range(len(kmers1))]
#     # bigsi1.build(blooms1, samples1)

#     # bigsi2 = BIGSI.create(db="./db-bigsi2/", m=10,
#     #                       k=9, h=1, force=True)
#     # blooms2 = []
#     # for s in kmers2:
#     #     blooms2.append(bigsi2.bloom([s]))
#     # samples2 = [str(i) for i in range(len(kmers2))]
#     # bigsi2.build(blooms2, samples2)

#     # combined_samples = combine_samples(samples1, samples2)
#     # bigsicombined = BIGSI.create(
#     #     db="./db-bigsi-c/", m=10, k=9, h=1, force=True)
#     # # bigsicombined = BIGSI(db="./db-bigsi-c/", mode="c")
#     # bigsicombined.build(blooms1+blooms2, combined_samples)

#     # bigsi1.merge(bigsi2)
#     # # bigsi1 = BIGSI(db="./db-bigsi1/")
#     # for i in range(10):
#     #     assert bigsi1.graph[i] == bigsicombined.graph[i]
#     # for k, v in bigsicombined.metadata.items():
#     #     assert bigsi1.metadata[k] == v
#     # bigsi1.delete_all()
#     # bigsi2.delete_all()
#     # bigsicombined.delete_all()
