#! /usr/bin/env python
from __future__ import print_function
from atlasseq.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import argparse
import json
import pickle
import collections


_stats = {}


def stats(conn_config):
    mc = Graph(storage={'redis-cluster': {"conn": conn_config,
                                          "array_size": 25000000,
                                          "num_hashes": 2}})
    # _stats["memory (bytes)"] = mc.calculate_memory()
    # _stats["keys"] = mc.count_keys()
    samples = list(mc.colours_to_sample_dict().values())
    _stats["kmer_count"] = mc.count_kmers(*samples)
    _stats["num_samples"] = mc.get_num_colours()

    return _stats
