import hypothesis.strategies as st
from bigsi import BIGSI
import os
import itertools


ROCKS_DB_CONFIG = {
    "bitarray-backend": {
        "type": "rocksdb",
        "filename": "test-rocksdb",
        "options": {"max_open_files": 5000, "create_if_missing": True},
    },
    "metadata-backend": {
        "type": "rocksdb",
        "filename": "test-rocksdb",
        "options": {"max_open_files": 5000, "create_if_missing": True},
    },
    "k": 31,
    "m": 25,
    "h": 3,
}

REDIS_CONFIG = {
    "bitarray-backend": {"type": "redis", "host": "localhost", "port": 6379},
    "metadata-backend": {"type": "redis", "host": "localhost", "port": 6379},
    "k": 31,
    "m": 25,
    "h": 3,
}

BERKELEY_DB_CONFIG = {
    "bitarray-backend": {"type": "berkeley-db"},
    "metadata-backend": {"type": "berkeley-db"},
    "k": 31,
    "m": 25,
    "h": 3,
}

CONFIGS = [ROCKS_DB_CONFIG]
L = ["".join(x) for x in itertools.product("ATCG", repeat=9)]
ST_KMER = st.sampled_from(L)
# ST_KMER_SIZE = st.integers(min_value=11, max_value=31)
ST_KMER_SIZE = st.just(13)

ST_SEQ = st.text(min_size=31, max_size=1000, alphabet=["A", "T", "C", "G"])

ST_SAMPLE_NAME = st.text(min_size=1)
# ST_GRAPH = st.just(BIGSI)
ST_SAMPLE_COLOUR = st.integers(min_value=0, max_value=10)
ST_BLOOM_FILTER_SIZE = st.integers(min_value=10, max_value=100)
ST_NUM_HASHES = st.integers(min_value=10, max_value=100)


BINARY_KMERS_OR_NOT = [True, False]
ST_BINARY_KMERS = st.just(False)  # st.sampled_from(BINARY_KMERS_OR_NOT)
