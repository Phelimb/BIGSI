#! /usr/bin/env python
from __future__ import print_function
from cbg.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import os.path
import sys
import logging
import json
logger = logging.getLogger(__name__)
from cbg.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)

import numpy as np
from bitarray import bitarray


def load_bloomfilter(f):
    bloomfilter = bitarray()
    with open(f, 'rb') as inf:
        bloomfilter.fromfile(inf)
    return bloomfilter


def build(bloomfilter_filepaths, samples, graph):
    bloomfilters = []
    for f in bloomfilter_filepaths:
        bloomfilters.append(load_bloomfilter(f))
    graph.build(bloomfilters, samples)
    return {'result': 'success'}
