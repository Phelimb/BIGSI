#! /usr/bin/env python
from __future__ import print_function
from cbg.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import os.path
import logging
import json
logger = logging.getLogger(__name__)
from cbg.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)

from pyseqfile import Reader
from cbg.utils import seq_to_kmers
from cbg.storage.graph.probabilistic import BloomFilterMatrix


def kmer_reader(f):
    count = 0
    reader = Reader(f)
    for i, line in enumerate(reader):
        if i % 100000 == 0:
            sys.stderr.write(str(i)+'\n')
            sys.stderr.flush()
        read = line.decode('utf-8')
        for k in seq_to_kmers(read):
            count += 1
            yield k
    sys.stderr.write(str(count))


def insert_kmers(mc, kmers, colour, sample, count_only=False):
    if not count_only:
        graph.insert_kmers(kmers, colour)
    graph.add_to_kmers_count(kmers, sample)


def bloom(outfile, kmers, kmer_file, graph):
    if kmer_file is not None:
        kmers = {}.fromkeys(kmer_reader(kmer_file)).keys()
    bloomfilter = graph.bloom(kmers)
    with open(outfile, 'wb') as of:
        bloomfilter.tofile(of)
