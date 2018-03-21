#! /usr/bin/env python
from __future__ import print_function
from bigsi.graph import BIGSI
import os.path
import sys
import logging
import json
logger = logging.getLogger(__name__)
from bigsi.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)

import numpy as np
from bitarray import bitarray
import math
from bigsi.utils import chunks
import tempfile


def load_bloomfilter(f):
    bloomfilter = bitarray()
    with open(f, 'rb') as inf:
        bloomfilter.fromfile(inf)
    return bloomfilter


def get_required_bytes_per_bloomfilter(m):
    return m * 9/8


def get_required_chunk_size(N, m, max_memory):
    bytes_per_bloomfilter = get_required_bytes_per_bloomfilter(m)
    required_bytes = bytes_per_bloomfilter*N
    num_chunks = math.ceil(required_bytes/max_memory)
    chunk_size = math.floor(N/num_chunks)
    return chunk_size, num_chunks


def build(bloomfilter_filepaths, samples, index, max_memory=None):
        # Max memory is in bytes
    if max_memory is None:
        chunk_size = len(bloomfilter_filepaths)
        num_chunks = 1
    else:
        chunk_size, num_chunks = get_required_chunk_size(
            N=len(samples), m=index.bloom_filter_size, max_memory=max_memory)
    if chunk_size < 1:
        raise ValueError(
            "Max memory must be at least 8 * Bloomfilter size in bytes")
    LL = list(zip(bloomfilter_filepaths, samples))
    for i, v in enumerate(chunks(LL, chunk_size)):
        bloomfilter_filepaths = [x[0] for x in v]
        samples = [x[1] for x in v]
        logger.info("Building index: %i/%i" % (i, num_chunks))
        if i == 0:
            build_main(bloomfilter_filepaths, samples, index)
        else:
            tmp_index = build_tmp(bloomfilter_filepaths, samples, index, i)
            index.merge(tmp_index)
            tmp_index.delete_all()
    return {'result': 'success'}


def build_main(bloomfilter_filepaths, samples, index):
    bloomfilters = []
    for f in bloomfilter_filepaths:
        bloomfilters.append(load_bloomfilter(f))
    index.build(bloomfilters, samples)


def build_tmp(bloomfilter_filepaths, samples, indext, i):
    index_dir = indext.db+"%i.tmp" % i
    index = BIGSI.create(db=index_dir, k=indext.kmer_size,
                         m=indext.bloom_filter_size, h=indext.num_hashes, force=True)
    build_main(bloomfilter_filepaths, samples, index)
    return BIGSI(index_dir)
