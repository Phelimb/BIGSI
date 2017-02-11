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


def load_memmap(filename):
    a = np.load(filename)
    return a


def flatten(l):
    return [item for sublist in l for item in sublist]


def merge(uncompressed_graphs, sizes, cols_list, outfile):
    ncols = sum([j for i, j in sizes])
    nrows = 10000  # sizes[0][0]
    _shape = (nrows, ncols)
    start = time.time()
    cols = flatten(cols_list)
    with open(outfile, 'wb') as outf:
        for batch in range(int(sizes[0][0]/nrows)):
            logger.info("batch %i of %i" % (batch, int(sizes[0][0]/nrows)))
            ugs = []
            for f in uncompressed_graphs:
                ug = load_memmap(f[str(batch)])
                ugs.append(ug)
            X = np.concatenate(ugs, axis=1)
            for row in X:
                ba_out = bitarray(row.tolist())
                outf.write(ba_out.tobytes())
    return {'graph': outfile, 'cols': cols}
