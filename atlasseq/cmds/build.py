#! /usr/bin/env python
from __future__ import print_function
from atlasseq.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import os.path
import sys
import logging
import json
logger = logging.getLogger(__name__)
from atlasseq.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)

import numpy as np
from bitarray import bitarray


def load_bloomfilter(f):
    bloomfilter = bitarray()
    with open(f, 'rb') as inf:
        bloomfilter.fromfile(inf)
    return np.array(bloomfilter)


def build(bloomfilter_filepaths, outfile, max_rows=10000):
    bloomfilters = []
    for f in bloomfilter_filepaths:
        bloomfilters.append(load_bloomfilter(f))
    bloomfilters = np.array(bloomfilters)
    _shape = (len(bloomfilters[0]), len(bloomfilters))
    bloomfilters = bloomfilters.transpose()
    # Calc number of output matrices
    d = {"shape": _shape, "uncompressed_graphs": {}}
    for i in range(int(_shape[0]/max_rows)):
        ii = i*max_rows
        jj = min((i+1)*max_rows, _shape[0])
        X = bloomfilters[ii:jj, :]
        chunk_outfile = "%s_rows_%i_to_%i" % (outfile, ii, jj)
        np.save(chunk_outfile, X)
        d["uncompressed_graphs"][i] = chunk_outfile+".npy"
    return d
