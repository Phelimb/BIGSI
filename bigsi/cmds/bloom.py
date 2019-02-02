#! /usr/bin/env python
from __future__ import print_function
from bigsi.graph import BIGSI
import os.path
import logging
import json

logger = logging.getLogger(__name__)
from bigsi.utils import DEFAULT_LOGGING_LEVEL

logger.setLevel(DEFAULT_LOGGING_LEVEL)
from bigsi.utils import seq_to_kmers


def bloom_file_name(f):
    f = os.path.realpath(f)
    return os.path.join(f, "_".join([os.path.basename(f)]))


def bloom(config, outfile, kmers):
    outfile = os.path.realpath(outfile)
    bloomfilter = BIGSI.bloom(config, kmers)
    off = bloom_file_name(outfile)
    directory = os.path.dirname(off)
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(off, "wb") as of:
        bloomfilter.tofile(of)
