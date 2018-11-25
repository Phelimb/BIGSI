from bigsi.tests.base import CONFIGS
from bigsi import BIGSI
from bigsi.storage import get_storage
from bitarray import bitarray
import pytest
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
    for config in CONFIGS:
        get_storage(config).delete_all()
        kmers_1 = seq_to_kmers("ATACACAAT", 3)
        kmers_2 = seq_to_kmers("ACAGAGAAC", 3)
        bloom1 = BIGSI.bloom(config, kmers_1)
        bloom2 = BIGSI.bloom(config, kmers_2)
        bigsi = BIGSI.build(config, [bloom1, bloom2], ["0", "1"])
        bigsi.search("ATACACAAT") == {
            0: {"percent_kmers": 100, "num_kmers": 6, "num_kmers_found": 6}
        }
        bigsi.search("ACAGAGAAC") == {
            1: {"percent_kmers": 100, "num_kmers": 6, "num_kmers_found": 6}
        }
        bigsi.search("ACAGTTAAC") == {}


# import copy


# def combine_samples(samples1, samples2):
#     combined_samples = copy.copy(samples1)
#     for x in samples2:
#         if x in combined_samples:
#             z = x + "_duplicate_in_merge"
#         else:
#             z = x
#         combined_samples.append(z)
#     return combined_samples


# # @given(kmers1=st.lists(ST_KMER, min_size=1, max_size=9), kmers2=st.lists(ST_KMER, min_size=1, max_size=9))
# # @settings(max_examples=1)
# # def test_merge(kmers1, kmers2):
# def test_merge():
#     kmers1 = ["AAAAAAAAA"] * 3
#     kmers2 = ["AAAAAAAAT"] * 9
#     bigsi1 = BIGSI.create(db="./db-bigsi1/", m=10, k=9, h=1, force=True)
#     blooms1 = []
#     for s in kmers1:
#         blooms1.append(bigsi1.bloom([s]))
#     samples1 = [str(i) for i in range(len(kmers1))]
#     bigsi1.build(blooms1, samples1)

#     bigsi2 = BIGSI.create(db="./db-bigsi2/", m=10, k=9, h=1, force=True)
#     blooms2 = []
#     for s in kmers2:
#         blooms2.append(bigsi2.bloom([s]))
#     samples2 = [str(i) for i in range(len(kmers2))]
#     bigsi2.build(blooms2, samples2)

#     combined_samples = combine_samples(samples1, samples2)
#     bigsicombined = BIGSI.create(db="./db-bigsi-c/", m=10, k=9, h=1, force=True)
#     # bigsicombined = BIGSI(db="./db-bigsi-c/", mode="c")
#     bigsicombined.build(blooms1 + blooms2, combined_samples)

#     bigsi1.merge(bigsi2)
#     # bigsi1 = BIGSI(db="./db-bigsi1/")
#     for i in range(10):
#         assert bigsi1.graph[i] == bigsicombined.graph[i]
#     for k, v in bigsicombined.metadata.items():
#         assert bigsi1.metadata[k] == v
#     bigsi1.delete_all()
#     bigsi2.delete_all()
#     bigsicombined.delete_all()


# def test_row_merge():
#     primary_db = "bigsi/tests/data/merge/test-bigsi-1"
#     try:
#         shutil.rmtree(primary_db)
#     except:
#         pass
#     shutil.copytree("bigsi/tests/data/merge/test-bigsi-1-init", primary_db)
#     bigsi1 = BIGSI("bigsi/tests/data/merge/test-bigsi-1")
#     bigsi2 = BIGSI("bigsi/tests/data/merge/test-bigsi-2")
#     bigsi3 = BIGSI("bigsi/tests/data/merge/test-bigsi-3")
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


# # @example(Graph=BIGSI, sample='0', seq='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA')


# #        score=st.sampled_from([True, False]))
# # @example(Graph=BIGSI, x=['AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAT', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAATA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG'], score=False)
