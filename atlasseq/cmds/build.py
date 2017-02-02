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


def build(bloomfilter_filepaths, outfile):
    bloomfilters = []
    for f in bloomfilter_filepaths:
        bloomfilters.append(load_bloomfilter(f))
    bloomfilters = np.array(bloomfilters)
    _shape = (len(bloomfilters[0]), len(bloomfilters))
    bloomfilters = bloomfilters.transpose()

    # fp = np.memmap(outfile, dtype='bool_', mode='w+',
    #                shape=_shape)
    # fp[:] = bloomfilters.transpose()
    # fp.flush()
    np.save(outfile, bloomfilters)
    return {"shape": _shape, "uncompressed_graph": outfile+'.npy'}
