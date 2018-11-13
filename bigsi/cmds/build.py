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
from bigsi.cmds.bloom import bloom_file_name
import tempfile


def load_bloomfilter(f, bf_range=None):
    ff = bloom_file_name(f, bf_range)
    logger.debug("Loading %s " % ff)
    bloomfilter = bitarray()
    with open(ff, "rb") as inf:
        bloomfilter.fromfile(inf)
    return bloomfilter[0 : bf_range[1] - bf_range[0]]


def get_required_bytes_per_bloomfilter(m):
    return m * 9 / 8


def get_required_chunk_size(N, m, max_memory):
    bytes_per_bloomfilter = get_required_bytes_per_bloomfilter(m)
    required_bytes = bytes_per_bloomfilter * N
    num_chunks = math.ceil(required_bytes / max_memory)
    chunk_size = math.floor(N / num_chunks)
    return chunk_size, num_chunks


def build(bloomfilter_filepaths, samples, index, max_memory=None, lowmem=False, bf_range=None):
    # Max memory is in bytes
    if max_memory is None:
        chunk_size = len(bloomfilter_filepaths)
        num_chunks = 1
    else:
        chunk_size, num_chunks = get_required_chunk_size(
            N=len(samples), m=index.bloom_filter_size, max_memory=max_memory
        )
    if chunk_size < 1:
        raise ValueError("Max memory must be at least 8 * Bloomfilter size in bytes")
    LL = list(zip(bloomfilter_filepaths, samples))
    for i, v in enumerate(chunks(LL, chunk_size)):
        bloomfilter_filepaths = [x[0] for x in v]
        # print(bloomfilter_filepaths)
        samples = [x[1] for x in v]
        logger.info("Building index: %i/%i" % (i, num_chunks))
        if i == 0:
            build_main(bloomfilter_filepaths, samples, index, lowmem=lowmem, bf_range=bf_range)
        else:
            tmp_index = build_tmp(bloomfilter_filepaths, samples, index, i, lowmem=lowmem, bf_range=bf_range)
            index.merge(tmp_index)
            tmp_index.delete_all()
    return {"result": "success"}


def build_main(bloomfilter_filepaths, samples, index, lowmem=False, bf_range=None):
    bloomfilters = []
    for f in bloomfilter_filepaths:
        bloomfilters.append(load_bloomfilter(f, bf_range))
    index.build(bloomfilters, samples, lowmem=lowmem, bf_range=bf_range)


def build_tmp(bloomfilter_filepaths, samples, indext, i, lowmem=False, bf_range=None):
    index_dir = indext.db + "%i.tmp" % i
    index = BIGSI.create(db=index_dir, k=indext.kmer_size, m=indext.bloom_filter_size, h=indext.num_hashes, force=True)
    build_main(bloomfilter_filepaths, samples, index, lowmem=lowmem, bf_range=bf_range)
    return index
