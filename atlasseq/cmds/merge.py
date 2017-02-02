#! /usr/bin/env python
from __future__ import print_function
from atlasseq.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import sys
import logging
import json
logger = logging.getLogger(__name__)
from atlasseq.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)

import numpy as np
from bitarray import bitarray
from tempfile import mkdtemp

import os
import psutil
process = psutil.Process(os.getpid())
import time


def load_memmap(filename, size):
    return np.load(filename, mmap_mode='r')
    # return np.memmap(filename, dtype='bool_', mode='r', shape=tuple(size))


def merge(uncompressed_graphs, sizes, outfile):
    ncols = sum([j for i, j in sizes])
    nrows = sizes[0][0]
    _shape = (nrows, ncols)
    start = time.time()
    ugs = []
    for f, size in zip(uncompressed_graphs, sizes):
        a = load_memmap(f, size)
        ugs.append(a)
    with open(outfile, 'wb') as outf:
        for i in range(nrows):
            a = np.append([], [ugs[j][i, ] for j in range(len(ugs))])
            if i % 10000 == 0:
                logger.info(i)
                logger.info(time.time()-start)
                logger.info("%i MB" % int(process.memory_info().rss/1000000))
            ba_out = bitarray(a.tolist())
            outf.write(ba_out.tobytes())
    return {'graph': outfile, "shape": _shape}
