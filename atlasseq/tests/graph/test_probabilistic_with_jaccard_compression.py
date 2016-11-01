

@given(
    binary_kmers=st_binary_kmers,
    primary_colour=st_sample_colour,
    secondary_colour=st_sample_colour,
    kmer=KMER)
def test_insert_secondary_kmer(binary_kmers, primary_colour, secondary_colour, kmer):

    mc = Graph(
        conn_config=conn_config,
        binary_kmers=binary_kmers, storage=probabilistic_redis_storage)
    mc.delete_all()
    if not primary_colour == secondary_colour:
        mc.insert_kmers([kmer], primary_colour)
        mc.insert_secondary_kmers([kmer], primary_colour, secondary_colour)
        # for i in range(mc.storage.array_size):
        #     assert not mc.lookup_primary_secondary_diff(primary_colour, i)
        assert [v for v in mc.get_kmer_primary_colours(
            kmer).values()] == [[primary_colour]]
        assert [v for v in mc.get_kmer_secondary_colours(primary_colour, kmer).values()] = [secondary_colour]
        assert [v for v in mc.get_kmer_colours(kmer).values()] == [
            [primary_colour, secondary_colour]]*len(kmer)

# @given(
#     binary_kmers=st_binary_kmers,
#     primary_colour=st_sample_colour,
#     secondary_colour=st_sample_colour,
#     kmers=st.lists(KMER, max_size=10))
# def test_insert_secondary_kmers2(binary_kmers, primary_colour, secondary_colour, kmers):
#     mc = Graph(
#         conn_config=conn_config,
#         binary_kmers=binary_kmers, storage=probabilistic_redis_storage)
#     mc.delete_all()
#     mc.insert_secondary_kmers(kmers, primary_colour, secondary_colour)
#     for i in range(mc.storage.array_size):
#         print(mc.lookup_primary_secondary_diff(primary_colour, i))
#         assert not mc.lookup_primary_secondary_diff(primary_colour, i)


@given(store=storage_no_berkeley, binary_kmers=st_binary_kmers, primary_colour=st_sample_colour, secondary_colour=st_sample_colour, diffs=st.lists(st_sample_colour, max_size=1000))
def test_insert_primary_secondary_diffs(store, binary_kmers, primary_colour, secondary_colour, diffs):
    mc = Graph(
        conn_config=conn_config, binary_kmers=binary_kmers, storage=store)
    mc.delete_all()
    mc.insert_primary_secondary_diffs(primary_colour, secondary_colour, diffs)
    for i in diffs:
        assert secondary_colour in mc.lookup_primary_secondary_diff(
            primary_colour, i)


@given(binary_kmers=st_binary_kmers, primary_colour=st_sample_colour, kmers=st.lists(KMER, max_size=100))
def test_get_diffs_between_primary_and_secondary_bloom_filter(binary_kmers, primary_colour, kmers):
    mc = Graph(
        conn_config=conn_config, binary_kmers=binary_kmers, storage=probabilistic_redis_storage)
    mc.delete_all()
    mc.insert_kmers(kmers, primary_colour)
    diffs = mc.diffs_between_primary_and_secondary_bloom_filter(
        primary_colour=primary_colour, kmers=kmers)
    assert diffs == []

# Todo - test this accross multiple backends


@given(kmers=st.lists(KMER, min_size=10, max_size=10, unique=True), binary_kmers=st_binary_kmers)
def test_jaccard_simillarity(kmers, binary_kmers):
    mc = Graph(
        conn_config=conn_config, binary_kmers=binary_kmers, storage={"probabilistic-redis": {"conn": [('localhost', 6379), ('localhost', 6380)], "array_size": 100000, "num_hashes": 2}})
    mc.delete_all()
    mc.add_sample('1234')
    mc.add_sample('1235')
    mc.insert_kmers(kmers, 0)
    mc.insert_kmers(kmers, 1)
    mc.add_to_kmers_count(kmers, sample='1234')
    mc.add_to_kmers_count(kmers, sample='1235')
    assert mc.jaccard_simillarity('1234', '1235') == 1


@given(kmers1=st.lists(KMER, min_size=10, max_size=10, unique=True),
       kmers2=st.lists(KMER, min_size=10, max_size=10, unique=True),
       binary_kmers=st_binary_kmers)
def test_jaccard_simillarity2(kmers1, kmers2, binary_kmers):
    mc = Graph(
        conn_config=conn_config, binary_kmers=binary_kmers, storage={"probabilistic-redis": {"conn": [('localhost', 6379), ('localhost', 6380)], "array_size": 100000, "num_hashes": 2}})
    mc.delete_all()
    mc.add_sample('1234')
    mc.add_sample('1235')
    mc.insert_kmers(kmers1, 0)
    mc.insert_kmers(kmers2, 1)
    mc.add_to_kmers_count(kmers1, sample='1234')
    mc.add_to_kmers_count(kmers2, sample='1235')
    skmers1 = set(kmers1)
    skmers2 = set(kmers2)
    true_sim = float(len(skmers1 & skmers2)) / float(len(skmers1 | skmers2))
    assert true_sim*.9 <= mc.jaccard_simillarity(
        '1234', '1235') <= true_sim*1.1


@given(kmers1=st.lists(KMER, min_size=10, max_size=10, unique=True),
       kmers2=st.lists(KMER, min_size=10, max_size=10, unique=True),
       binary_kmers=st_binary_kmers)
def test_kmer_diff(kmers1, kmers2, binary_kmers):
    mc = Graph(
        conn_config=conn_config, binary_kmers=binary_kmers, storage={"probabilistic-redis": {"conn": [('localhost', 6379), ('localhost', 6380)], "array_size": 100000, "num_hashes": 2}})
    mc.delete_all()
    mc.add_sample('1234')
    mc.add_sample('1235')
    mc.insert_kmers(kmers1, 0)
    mc.insert_kmers(kmers2, 1)
    mc.add_to_kmers_count(kmers1, sample='1234')
    mc.add_to_kmers_count(kmers2, sample='1235')
    skmers1 = set(kmers1)
    skmers2 = set(kmers2)
    true_diff = float(len(skmers1 ^ skmers2))
    # true_diff2 = float(len(skmers2 - skmers1))
    assert true_diff*.9 <= mc.symmetric_difference(
        '1234', '1235') <= true_diff*1.1
    true_diff = float(len(skmers1 - skmers2))

    assert true_diff*.9 <= mc.difference(
        '1234', '1235') <= true_diff*1.1
    true_diff = float(len(skmers2 - skmers1))

    assert true_diff*.9 <= mc.difference(
        '1235', '1234') <= true_diff*1.1
