import hypothesis.strategies as st
from bigsi import BIGSI
import os
import itertools
REDIS_HOST = os.environ.get("REDIS_IP_1", '127.0.0.1')
REDIS_CLUSTER_PORT = os.environ.get("REDIS_PORT_1", '7000')
REDIS_PORT = 6379
REDIS_CLUSTER_STORAGE_REDIS = {
    "conn": [(REDIS_HOST, REDIS_CLUSTER_PORT, 0)], 'credis': False}
REDIS_CLUSTER_STORAGE_CREDIS = {
    "conn": [(REDIS_HOST, REDIS_CLUSTER_PORT, 0)], 'credis': True}

L = ["".join(x) for x in itertools.product("ATCG", repeat=9)]
ST_KMER = st.sampled_from(L)
# ST_KMER_SIZE = st.integers(min_value=11, max_value=31)
ST_KMER_SIZE = st.just(13)

ST_SEQ = st.text(min_size=31, max_size=1000, alphabet=['A', 'T', 'C', 'G'])

ST_SAMPLE_NAME = st.text(min_size=1)
ST_GRAPH = st.just(BIGSI)
ST_SAMPLE_COLOUR = st.integers(min_value=0, max_value=10)
ST_BLOOM_FILTER_SIZE = st.integers(min_value=10, max_value=100)
ST_NUM_HASHES = st.integers(min_value=10, max_value=100)
POSSIBLE_STORAGES = [
    # {'dict': None},
    # {"redis": {"conn": [(REDIS_HOST, REDIS_PORT, 2)]}},
    # {"redis-cluster": REDIS_CLUSTER_STORAGE_REDIS},
    # {"redis-cluster": REDIS_CLUSTER_STORAGE_CREDIS},
    {'berkeleydb': {'filename': './db'}},
    # {'leveldb': {'filename': './db2'}}
]

PERSISTANT_STORAGES = [
    # {"redis": {"conn": [(REDIS_HOST, REDIS_PORT, 2)]}},
    # {"redis-cluster": REDIS_CLUSTER_STORAGE_REDIS},
    # {"redis-cluster": REDIS_CLUSTER_STORAGE_CREDIS},

    {'berkeleydb': {'filename': './db'}},
    # {'leveldb': {'filename': './db2'}}
]
ST_PERSISTANT_STORAGE = st.sampled_from(PERSISTANT_STORAGES)

BINARY_KMERS_OR_NOT = [True, False]
ST_BINARY_KMERS = st.just(False)  # st.sampled_from(BINARY_KMERS_OR_NOT)
