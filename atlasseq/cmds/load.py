#! /usr/bin/env python
from __future__ import print_function
import logging
import json
logger = logging.getLogger(__name__)
from atlasseq.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)



def load(graph, file):
    logger.debug("Loading from %s" % file)
    with open(file, 'rb') as outfile:
        graph.load(outfile)
    return {'result': 'success'}
