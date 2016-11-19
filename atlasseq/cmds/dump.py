#! /usr/bin/env python
from __future__ import print_function
from atlasseq.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import argparse
import json
import logging
import json
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def dump(graph, file):
    logger.debug("Dumping graph tp %s" % file)
    with open(file, 'wb') as outfile:
        graph.dump(outfile)
    return {'result': 'success'}
