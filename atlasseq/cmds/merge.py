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


def merge(graph, uncompressed_graphs, indexes, cols_list, outdir, force=False):
    start = time.time()
    cols = flatten(cols_list)
    outfiles = {}
    for batch in indexes:
        logger.info("batch %i of %i" % (batch, max(indexes)))
        ugs = []
        for f in uncompressed_graphs:
            ug = load_memmap(f[str(batch)])
            ugs.append(ug)
        X = np.concatenate(ugs, axis=1)
        for i, row in enumerate(X):
            j = i + batch*10000
            ba_out = bitarray(row.tolist())
            directory = "/".join([outdir, str(batch)])
            if not os.path.exists(directory):
                os.makedirs(directory)
            outfile = "/".join([directory, "row_%i" % j])
            outfiles[j] = outfile
            with open(outfile, 'wb') as outf:
                outf.write(ba_out.tobytes())
    return {'graph': outfiles, 'cols': cols}
