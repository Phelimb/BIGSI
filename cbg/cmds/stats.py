#! /usr/bin/env python
from __future__ import print_function
from cbg.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import argparse
import json
import pickle
import collections


_stats = {}


def stats(graph):

    # _stats["memory (bytes)"] = graph.calculate_memory()
    # _stats["keys"] = graph.count_keys()
    samples = list(graph.sample_to_colour_lookup.keys())
    _stats["kmer_count"] = graph.count_kmers(*samples)
    _stats["num_samples"] = graph.get_num_colours()

    return _stats
