#! /usr/bin/env python
from __future__ import print_function
from bfg.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import os.path
import sys
import logging
import json
logger = logging.getLogger(__name__)
from bfg.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)

from bfg.tasks import run_insert


def insert(kmers, kmer_file, merge_results, graph, force=False, sample_name=None,
           intersect_kmers_file=None, sketch_only=False, async=False):
    if async:
        logger.debug("Inserting with a celery task")
        result = run_insert.delay(kmers, kmer_file, merge_results, graph.storage, graph.bloom_filter_size,
                                  graph.num_hashes, force=force, sample_name=sample_name,
                                  intersect_kmers_file=intersect_kmers_file, sketch_only=sketch_only)
        result = result.get()
    else:
        logger.debug("Inserting without a celery task")
        result = run_insert(kmers, kmer_file, merge_results, graph.storage, graph.bloom_filter_size, graph.num_hashes, force=force, sample_name=sample_name,
                            intersect_kmers_file=intersect_kmers_file, sketch_only=sketch_only)
    return result
