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

from atlasseq.tasks import run_insert


def insert(kmers, kmer_file, graph, force=False, sample_name=None,
           intersect_kmers_file=None, sketch_only=False, async=False):
    if async:
        result = run_insert.delay(kmers, kmer_file, graph.storage, graph.bloom_filter_size, graph.num_hashes, force=force, sample_name=sample_name,
                                  intersect_kmers_file=intersect_kmers_file, sketch_only=sketch_only).get()
    else:
        result = run_insert(kmers, kmer_file, graph.storage, graph.bloom_filter_size, graph.num_hashes, force=force, sample_name=sample_name,
                            intersect_kmers_file=intersect_kmers_file, sketch_only=sketch_only)
    return result
