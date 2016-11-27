import hypothesis.strategies as st
from atlasseq import ProbabilisticMultiColourDeBruijnGraph
import os
REDIS_HOST = os.environ.get("REDIS_IP_1", '127.0.0.1')
REDIS_CLUSTER_PORT = os.environ.get("REDIS_PORT_1", '7000')
REDIS_PORT = 6379
REDIS_CLUSTER_STORAGE = {
    "conn": [(REDIS_HOST, REDIS_CLUSTER_PORT, 0)], 'credis': False}
REDIS_CLUSTER_STORAGE = {
    "conn": [(REDIS_HOST, REDIS_CLUSTER_PORT, 0)], 'credis': True}

ST_KMER = st.text(min_size=31, max_size=31, alphabet=['A', 'T', 'C', 'G'])
ST_SAMPLE_NAME = st.text(min_size=1)
ST_GRAPH = st.sampled_from([ProbabilisticMultiColourDeBruijnGraph])
ST_SAMPLE_COLOUR = st.integers(min_value=0, max_value=10)

POSSIBLE_STORAGES = [
    # {'dict': None},
    # {"redis": {"conn": [(REDIS_HOST, REDIS_PORT, 2)]}},
    {"redis-cluster": REDIS_CLUSTER_STORAGE},
    {"redis-cluster": REDIS_CLUSTER_STORAGE},
    {'berkeleydb': {'filename': './db'}},
    # {'leveldb': {'filename': './db2'}}
]

PERSISTANT_STORAGES = [
    # {"redis": {"conn": [(REDIS_HOST, REDIS_PORT, 2)]}},
    {"redis-cluster": REDIS_CLUSTER_STORAGE},

    # {'berkeleydb': {'filename': './db'}},
    # {'leveldb': {'filename': './db2'}}
]
ST_STORAGE = st.sampled_from(POSSIBLE_STORAGES)
ST_PERSISTANT_STORAGE = st.sampled_from(PERSISTANT_STORAGES)

BINARY_KMERS_OR_NOT = [True, False]
ST_BINARY_KMERS = st.sampled_from(BINARY_KMERS_OR_NOT)
