#! /usr/bin/env python
from __future__ import print_function
from bigsi.graph import BIGSI as Graph
import os.path
import logging
import json

logger = logging.getLogger(__name__)
from bigsi.utils import DEFAULT_LOGGING_LEVEL

logger.setLevel(DEFAULT_LOGGING_LEVEL)

from pyseqfile import Reader
from bigsi.utils import seq_to_kmers


def bloom_file_name(f, bf_range):
    f = os.path.realpath(f)
    return os.path.join(
        f, "_".join([os.path.basename(f), str(bf_range[0]), str(bf_range[1])])
    )


def kmer_reader(f):
    count = 0
    reader = Reader(f)
    for i, line in enumerate(reader):
        if i % 100000 == 0:
            sys.stderr.write(str(i) + "\n")
            sys.stderr.flush()
        read = line.decode("utf-8")
        for k in seq_to_kmers(read):
            count += 1
            yield k
    sys.stderr.write(str(count))


def bloom(config, outfile, kmer_file):
    outfile = os.path.realpath(outfile)
    kmers = {}.fromkeys(kmer_reader(kmer_file)).keys()
    bloomfilter = BIGSI.bloom(config, kmers)
    off = bloom_file_name(outfile, (i, j))
    directory = os.path.dirname(off)
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(off, "wb") as of:
        bloomfilter[i:j].tofile(of)
