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


def load_memmap(filename, rowi, rowj):
    logger.info("%i MB" % int(process.memory_info().rss/1000000))
    return np.load(filename, mmap_mode='r')[rowi:rowj, :]
    # return np.memmap(filename, dtype='bool_', mode='r', shape=tuple(size))


def merge(uncompressed_graphs, sizes, outfile):
    ncols = sum([j for i, j in sizes])
    nrows = 10000  # sizes[0][0]
    print(int(sizes[0][0]/nrows))
    _shape = (nrows, ncols)
    start = time.time()

    with open(outfile, 'wb') as outf:
        for batch in range(int(sizes[0][0]/nrows)):
            ugs = []
            for f, size in zip(uncompressed_graphs, sizes):
                a = load_memmap(f, 0 + (batch*nrows), nrows+(batch*nrows))
                ugs.append(a)
            for i in range(nrows):
                a = np.append([], [ugs[j][i, ] for j in range(len(ugs))])
                if (batch*nrows + i) % 10000 == 0:
                    logger.info(batch*nrows + i)
                    logger.info(time.time()-start)
                    logger.info("%i MB" %
                                int(process.memory_info().rss/1000000))
                ba_out = bitarray(a.tolist())
                outf.write(ba_out.tobytes())
    return {'graph': outfile, "shape": _shape}
