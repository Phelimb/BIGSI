from bigsi.tests.base import CONFIGS
from bigsi import BIGSI
from bigsi.storage import get_storage
from bitarray import bitarray


def test_create():
    for config in CONFIGS:
        get_storage(config).delete_all()
        bloomfilters = [BIGSI.bloom(config, ["ATC", "ATA"])]
        samples = ["1"]
        bigsi = BIGSI.build(config, bloomfilters, samples)
        assert bigsi.kmer_size == 3
        assert bigsi.bloomfilter_size == 25
        assert bigsi.num_hashes == 3
        assert bigsi.num_samples == 1
        assert bigsi.lookup("ATC") == {"ATC": bitarray("1")}
        bigsi.delete()


def test_insert():
    for config in CONFIGS:
        print(config)
        get_storage(config).delete_all()
        bloomfilters = [BIGSI.bloom(config, ["ATC", "ATA"])]
        samples = ["1"]
        bigsi = BIGSI.build(config, bloomfilters, samples)
        bloomfilter_2 = BIGSI.bloom(config, ["ATC", "ATT"])
        print(bloomfilters[0], bloomfilter_2)
        bigsi.insert(bloomfilter_2, "2")
        assert bigsi.kmer_size == 3
        assert bigsi.bloomfilter_size == 25
        assert bigsi.num_hashes == 3
        assert bigsi.num_samples == 2
        print(bigsi.lookup(["ATC", "ATA", "ATT"]))
        assert bigsi.lookup(["ATC", "ATA", "ATT"]) == {
            "ATC": bitarray("11"),
            "ATA": bitarray("10"),
            "ATT": bitarray("01"),
        }


# def test_unique_sample_names():
#     Graph, sample = BIGSI, "0"
#     seq, k, h = "AATTTTTATTTTTTTTTTTTTAATTAATATT", 11, 1
#     m = 10
#     logger.debug("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
#     kmers = seq_to_kmers(seq, k)
#     bigsi = Graph.create(m=m, k=k, h=h, force=True)
#     assert bigsi.kmer_size == k
#     bloom = bigsi.bloom(kmers)
#     assert len(bloom) == m
#     bigsi.build([bloom], [sample])
#     with pytest.raises(ValueError):
#         bigsi.insert(bloom, sample)
#     assert sample in bigsi.search(seq)
#     assert bigsi.search(seq).get(sample).get("percent_kmers_found") == 100
#     bigsi.delete_all()


# # def test_cant_write_to_read_only_index():
# #     Graph, sample = BIGSI, "sfewe"
# #     seq, k, h = 'AATTTTTATTTTTTTTTTTTTAATTAATATT', 11, 1
# #     m = 10
# #     logger.debug("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
# #     kmers = seq_to_kmers(seq, k)
# #     bigsi = Graph.create(m=m, k=k, h=h, force=True)
# #     assert bigsi.kmer_size == k
# #     bloom = bigsi.bloom(kmers)
# #     bigsi.build([bloom], [sample])
# #     os.chmod(bigsi.graph_filename, S_IREAD | S_IRGRP | S_IROTH)
# #     # Can write to a read only DB
# #     bigsi = Graph(mode="r")
# #     with pytest.raises(bsddb3.db.DBAccessError):
# #         bigsi.insert(bloom, "1234")
# #     assert sample in bigsi.search(seq)
# #     assert bigsi.search(seq).get(sample).get('percent_kmers_found') == 100
# #     os.chmod(bigsi.graph_filename, S_IWUSR | S_IREAD)
# #     bigsi.delete_all()


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


# def test_insert_lookup_kmers():
#     Graph, sample, seq = BIGSI, "0", "AAAAAAAAAAAATCAAAAAAAAAAAAAAAAA"
#     m, h, k = 10, 2, 31

#     logger.debug("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
#     kmers = list(seq_to_kmers(seq, k))
#     bigsi = Graph.create(m=m, k=k, h=h, force=True)
#     bloom = bigsi.bloom(kmers)
#     bigsi.build([bloom], [sample])
#     for kmer in kmers:
#         # assert sample not in bigsi.lookup(kmer+"T")[kmer+"T"]
#         ba = bitarray()
#         ba.frombytes(bigsi.lookup_raw(kmer))
#         assert ba[0] == True
#         assert sample in bigsi.lookup(kmer)[kmer]
#     assert [sample] in bigsi.lookup(kmers).values()
#     bigsi.delete_all()


# # TODO update for insert to take bloomfilter
# # @example(Graph=BIGSI, kmer='AAAAAAAAA')
# # def test_insert_get_kmer(Graph, kmer):
# def test_insert_get_kmer():
#     Graph, kmer = BIGSI, "AAAAAAAAA"
#     bigsi = Graph.create(m=10, force=True)
#     bloom = bigsi.bloom([kmer])
#     bigsi.build([bloom], ["1"])
#     assert bigsi.colours(kmer)[kmer] == [0]
#     bigsi.insert(bloom, "2")
#     assert bigsi.colours(kmer)[kmer] == [0, 1]
#     bigsi.delete_all()


# # def test_query_kmer(Graph, kmer):
# def test_query_kmer():
#     Graph, kmer = BIGSI, "AAAAAAAAA"
#     bigsi = Graph.create(m=100, force=True)
#     bloom1 = bigsi.bloom([kmer])
#     bigsi.build([bloom1], ["1234"])
#     assert bigsi.lookup(kmer) == {kmer: ["1234"]}
#     bigsi.insert(bloom1, "1235")
#     assert bigsi.lookup(kmer) == {kmer: ["1234", "1235"]}
#     bigsi.delete_all()


# @given(
#     s=ST_SAMPLE_NAME,
#     key=st.text(min_size=1),
#     value=st.text(min_size=1),
#     value2=st.one_of(
#         st.text(min_size=1),
#         st.dictionaries(keys=st.text(min_size=1), values=st.text(min_size=1)),
#         st.lists(st.integers()),
#         st.sets(st.integers()),
#     ),
# )
# @settings(max_examples=2)
# def test_add_metadata(Graph, s, key, value, value2):
#     kmer = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
#     bigsi = Graph.create(m=100, force=True)
#     bloom1 = bigsi.bloom([kmer])
#     bigsi.build([bloom1], [s])
#     bigsi.add_sample_metadata(s, key, value)
#     assert bigsi.lookup_sample_metadata(s).get(key) == value
#     with pytest.raises(ValueError):
#         bigsi.add_sample_metadata(s, key, value2)
#     assert bigsi.lookup_sample_metadata(s).get(key) == value
#     # Key already exists
#     bigsi.add_sample_metadata(s, key, value2, overwrite=True)
#     assert bigsi.lookup_sample_metadata(s).get(key) == value2

#     bigsi.delete_all()


# #        score=st.sampled_from([True, False]))
# # @example(Graph=BIGSI, x=['AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAT', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAATA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG'], score=False)
# def test_query_kmers():
#     Graph = BIGSI
#     x = [
#         "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
#         "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAT",
#         "AAAAAAAAAAAAAAAAAAAAAAAAAAAAATA",
#         "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC",
#         "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG",
#     ]
#     score = False
#     m = 100
#     h = 2
#     k = 9
#     logger.debug("Testing graph with params (k=%i,m=%i,h=%i)" % (k, m, h))
#     logger.debug("Testing graph kmers %s" % ",".join(x))
#     k1, k2, k3, k4, k5 = x
#     print("first create call")
#     bigsi = Graph.create(m=m, k=k, h=h, force=True)

#     bloom1 = bigsi.bloom([k1, k2, k5])
#     bloom2 = bigsi.bloom([k1, k3, k5])
#     bloom3 = bigsi.bloom([k4, k3, k5])

#     bigsi.build([bloom1, bloom2, bloom3], ["1234", "1235", "1236"])

#     assert bigsi.get_num_colours() == 3
#     bigsi.num_colours = bigsi.get_num_colours()
#     logger.debug("Searching graph score %s" % str(score))

#     assert (
#         bigsi._search([k1, k2], threshold=0.5, score=score)
#         .get("1234")
#         .get("percent_kmers_found")
#         >= 100
#     )
#     assert (
#         bigsi._search([k1, k2], threshold=0.5, score=score)
#         .get("1235")
#         .get("percent_kmers_found")
#         >= 50
#     )

#     assert (
#         bigsi._search([k1, k2], score=score).get("1234").get("percent_kmers_found")
#         >= 100
#     )

#     assert (
#         bigsi._search([k1, k3], threshold=0.5, score=score)
#         .get("1234")
#         .get("percent_kmers_found")
#         >= 50
#     )
#     assert (
#         bigsi._search([k1, k3], threshold=0.5, score=score)
#         .get("1235")
#         .get("percent_kmers_found")
#         >= 100
#     )
#     assert (
#         bigsi._search([k1, k3], threshold=0.5, score=score)
#         .get("1236")
#         .get("percent_kmers_found")
#         >= 50
#     )

#     assert (
#         bigsi._search([k5], score=score).get("1234").get("percent_kmers_found") >= 100
#     )
#     assert (
#         bigsi._search([k5], score=score).get("1235").get("percent_kmers_found") >= 100
#     )
#     assert (
#         bigsi._search([k5], score=score).get("1236").get("percent_kmers_found") >= 100
#     )

#     bigsi.delete_all()
