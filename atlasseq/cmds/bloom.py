#! /usr/bin/env python
from __future__ import print_function
from atlasseq.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import os.path
import logging
import json
logger = logging.getLogger(__name__)
from atlasseq.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)

from pyseqfile import Reader
from atlasseq.utils import seq_to_kmers
from atlasseq.utils import kmer_reader
from atlasseq.utils import unique_kmers


def insert_kmers(mc, kmers, colour, sample, count_only=False):
    if not count_only:
        graph.insert_kmers(kmers, colour)
    graph.add_to_kmers_count(kmers, sample)


def bloom(kmers, kmer_file, graph):
    if kmer_file is not None:
        kmers = unique_kmers(kmer_file)
    # for k in kmers:
    #     k
    bloom_filter = graph.create_bloom_filter(kmers)
    return bloom_filter.tobytes()
    # return b''
