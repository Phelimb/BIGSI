#! /usr/bin/env python
from __future__ import print_function
from atlasseq.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import argparse
import json
import pickle
import collections


_stats = {}


def stats(conn_config):
    mc = McDBG(conn_config=conn_config, storage={'probabilistic-redis': {"conn": conn_config,
                                                                         "array_size": 25000000, "num_hashes": 2}})
    _stats["memory (bytes)"] = mc.calculate_memory()
    _stats["keys"] = mc.count_keys()
    _stats["kmers"] = mc.count_kmers()
    _stats["samples"] = mc.get_num_colours()

    return _stats  # collections.OrderedDict(sorted(.items()))
