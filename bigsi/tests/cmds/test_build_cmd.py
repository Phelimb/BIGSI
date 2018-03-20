import os
import hug
import redis

import bigsi.__main__
import json
from bigsi.tests.base import ST_SEQ
from bigsi.tests.base import ST_KMER
from bigsi.tests.base import ST_SAMPLE_NAME
from bigsi.tests.base import ST_GRAPH
from bigsi import BIGSI
import hypothesis.strategies as st
from hypothesis import given
from hypothesis import settings

import random
import tempfile

from bigsi.cmds.build import get_required_bytes_per_bloomfilter
from bigsi.cmds.build import get_required_chunk_size
from bigsi.cmds.build import build
from bitarray import bitarray
import numpy as np
import string
import pytest


def test_get_required_bytes_per_bloomfilter():
    m = 25*10**6
    num_bytes = get_required_bytes_per_bloomfilter(m)
    assert num_bytes == 25*10**6 + (25*10**6)/8


def test_get_required_chunk_size():
    m = 25*10**6
    N = 10
    max_memory = 10**8  # 100MB
    assert get_required_chunk_size(m=m, N=N, max_memory=max_memory) == 3

    m = 25*10**6
    N = 10
    max_memory = 10**9  # 100MB
    assert get_required_chunk_size(m=m, N=N, max_memory=max_memory) == 10


def generate_sample_names(N):
    samples = []
    for i in range(N):
        samples.append(''.join(random.choice(
            string.ascii_uppercase + string.digits) for _ in range(6)))
    return samples


def test_build_chunks():
    N = 3
    bloomfilter_filepaths = ['bigsi/tests/data/test_kmers.bloom']*N
    sample_names = generate_sample_names(
        len(bloomfilter_filepaths))

    bigsi1 = BIGSI.create(db="./db-bigsi-no-max-mem/",
                          m=10, k=9, h=1, force=True)
    build(bloomfilter_filepaths, sample_names, bigsi1)

    bigsi2 = BIGSI.create(db="./db-bigsi-max-mem/", m=10, k=9, h=1, force=True)
    build(bloomfilter_filepaths, sample_names,
          bigsi2, max_memory=20)  # 20bytes

    # Reload and test equal
    bigsi1 = BIGSI("./db-bigsi-no-max-mem/")
    bigsi2 = BIGSI("./db-bigsi-max-mem")
    for i in range(10):
        assert bigsi1.graph[i] == bigsi2.graph[i]
    for k, v in bigsi2.metadata.items():
        assert bigsi1.metadata[k] == v

    bigsi1.delete_all()
    bigsi2.delete_all()


def test_cant_build_chunks_if_max_memory_less_than_bf():
    N = 3
    bloomfilter_filepaths = ['bigsi/tests/data/test_kmers.bloom']*N
    sample_names = generate_sample_names(
        len(bloomfilter_filepaths))

    bigsi2 = BIGSI.create(db="./db-bigsi-max-mem/", m=10, k=9, h=1, force=True)
    with pytest.raises(ValueError):
        build(bloomfilter_filepaths, sample_names,
              bigsi2, max_memory=1)  # 1byte (should fail)
