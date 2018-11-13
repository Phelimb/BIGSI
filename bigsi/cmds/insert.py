#! /usr/bin/env python
from __future__ import print_function
from bigsi.graph import BIGSI as Graph
import os.path
import sys
import logging
import json

logger = logging.getLogger(__name__)
from bigsi.utils import DEFAULT_LOGGING_LEVEL

logger.setLevel(DEFAULT_LOGGING_LEVEL)
from bitarray import bitarray
from bigsi.cmds.build import load_bloomfilter

# def load_bloomfilter(f):
#     bloomfilter = bitarray()
#     with open(f, 'rb') as inf:
#         bloomfilter.fromfile(inf)
#     return bloomfilter


def insert(graph, bloomfilter, sample, bf_range):
    ## To do add warning that build is normally preferable
    graph.insert(load_bloomfilter(bloomfilter, bf_range=bf_range), sample)
    return {"result": "success"}
