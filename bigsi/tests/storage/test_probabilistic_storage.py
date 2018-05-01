from bigsi.tests.base import ST_KMER
from bigsi.tests.base import ST_SEQ
from bigsi.tests.base import ST_SAMPLE_NAME
from bigsi.tests.base import ST_GRAPH
from bigsi.tests.base import ST_BLOOM_FILTER_SIZE
from bigsi.tests.base import ST_SAMPLE_COLOUR
from bigsi.tests.base import ST_NUM_HASHES
import hypothesis
from hypothesis import given
from hypothesis import settings
import hypothesis.strategies as st
from bigsi.storage.graph.probabilistic import ProbabilisticBerkeleyDBStorage
from bigsi.utils import seq_to_kmers

import os


@given(colour=ST_SAMPLE_COLOUR, element=ST_KMER,
       bloom_filter_size=ST_BLOOM_FILTER_SIZE,
       num_hashes=ST_NUM_HASHES)
def test_add_contains(colour, element, bloom_filter_size,  num_hashes):
    storage = ProbabilisticBerkeleyDBStorage(filename="db",
                                             bloom_filter_size=bloom_filter_size,
                                             num_hashes=num_hashes,
                                             mode="c")
    storage.bloom_filter_size = bloom_filter_size
    storage.num_hashes = num_hashes

    storage.bloomfilter.add(element, colour)
    assert storage.bloomfilter.contains(element, colour)
    storage.delete_all()


@given(colour=ST_SAMPLE_COLOUR, elements=ST_SEQ,
       bloom_filter_size=ST_BLOOM_FILTER_SIZE,
       num_hashes=ST_NUM_HASHES)
def test_update_contains(colour, elements, bloom_filter_size,  num_hashes):
    storage = ProbabilisticBerkeleyDBStorage(filename="db",
                                             bloom_filter_size=bloom_filter_size,
                                             num_hashes=num_hashes,
                                             mode="c")

    elements = list(seq_to_kmers(elements, 31))
    storage.bloom_filter_size = bloom_filter_size
    storage.num_hashes = num_hashes

    storage.bloomfilter.update(elements, colour)
    for k in elements:
        assert storage.bloomfilter.contains(k, colour)
    storage.delete_all()

# TODO fix this test. Not sure if we should allow updates without
# appending a new column

# @given(colour1=ST_SAMPLE_COLOUR, colour2=ST_SAMPLE_COLOUR,
#        element=ST_KMER,
#        bloom_filter_size=st.integers(10000, 1000000),
#        num_hashes=st.integers(min_value=1, max_value=5))
# def test_add_lookup(storage, colour1, colour2, element, bloom_filter_size,  num_hashes):
#     storage.bloom_filter_size = bloom_filter_size
#     storage.num_hashes = num_hashes
#     storage.delete_all()
#     array_size = max([colour1, colour2])+1

#     if not colour1 == colour2:
#         storage.bloomfilter.add(element, colour1)
#         assert storage.bloomfilter.contains(element, colour1)
#         assert not storage.bloomfilter.contains(element, colour2)
#         assert storage.bloomfilter.lookup(
#             element, array_size).getbit(colour1) == True
#         assert storage.bloomfilter.lookup(
#             element, array_size).getbit(colour2) == False
#         storage.bloomfilter.add(element, colour2)
#         assert storage.bloomfilter.contains(element, colour1)
#         assert storage.bloomfilter.contains(element, colour2)
#         assert storage.bloomfilter.lookup(
#             element, array_size).getbit(colour1) == True
#         assert storage.bloomfilter.lookup(
#             element, array_size).getbit(colour2) == True

# TODO fix this test. Not sure if we should allow updates without appending a new column
# @given(storage=ST_STORAGE,
#        elements=st.text(
#            min_size=31, max_size=100, alphabet=['A', 'T', 'C', 'G']),
#        bloom_filter_size=st.integers(1000, 10000))
# @settings(suppress_health_check=hypothesis.errors.Timeout)
# def test_add_lookup_list(storage, elements, bloom_filter_size):
#     num_hashes = 3
#     colour1 = 0
#     colour2 = 1
#     elements = list(seq_to_kmers(elements))
#     storage.bloom_filter_size = bloom_filter_size
#     storage.num_hashes = num_hashes
#     storage.delete_all()
#     if not colour1 == colour2:
#         storage.bloomfilter.update(elements, colour1)
#         assert all([storage.bloomfilter.contains(element, colour1)
#                     for element in elements])
#         assert all([not storage.bloomfilter.contains(element, colour2)
#                     for element in elements])
#         array_size = max(colour1, colour2)+1
#         assert all([storage.bloomfilter.lookup(elements, array_size)[i].getbit(colour1)
#                     for i in range(len(elements))]) == True
#         assert all([storage.bloomfilter.lookup(elements, array_size)[i].getbit(colour2) == False
#                     for i in range(len(elements))])

#         storage.bloomfilter.update(elements, colour2)
#         assert all([storage.bloomfilter.contains(element, colour1)
#                     for element in elements])
#         assert all([storage.bloomfilter.contains(element, colour2)
#                     for element in elements])
#         assert all([storage.bloomfilter.lookup(elements, array_size)[i].getbit(
#             colour1) == True for i in range(len(elements))])
#         assert all([storage.bloomfilter.lookup(elements, array_size)[i].getbit(
#             colour2) == True for i in range(len(elements))])
