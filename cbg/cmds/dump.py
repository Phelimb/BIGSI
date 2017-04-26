#! /usr/bin/env python
from __future__ import print_function
from cbg.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import argparse
import json
import logging
import json
logger = logging.getLogger(__name__)
from cbg.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)


def dump(graph, file):
    logger.debug("Dumping graph tp %s" % file)
    graph.dump(file)
    return {'result': 'success'}
