#! /usr/bin/env python
from __future__ import print_function
from atlasseq.mcdbg import McDBG
import argparse
import json
import pickle
import collections
import itertools

_stats = {}


def calc_ji(mc, s1, s2):
    d = {}
    d["jaccard-index"] = mc.jaccard_index(s1, s2)
    d[
        "jaccard-distance"] = mc.jaccard_distance(s1, s2)
    d[
        "symmetric_difference"] = mc.symmetric_difference(s1, s2)
    d["num-kmers-unique-to-%s" %
      s1] = mc.difference(s1, s2)
    d["num-kmers-unique-to-%s" %
      s2] = mc.difference(s2, s1)
    d["sample-1"] = s1
    d["sample-2"] = s2
    return d


def jaccard_index(s1=None, s2=None, conn_config=None):
    mc = McDBG(conn_config=conn_config, storage={'probabilistic-redis': {"conn": conn_config,
                                                                         "array_size": 25000000, "num_hashes": 2}})
    if s1 is None and s2 is None:
        samples = mc.colours_to_sample_dict().values()
        for pair in list(itertools.combinations(samples, 2)):
            s1, s2 = pair
            try:
                _stats[s1][s2] = calc_ji(mc, s1, s2)
            except KeyError:
                _stats[s1] = {}
                _stats[s1][s2] = calc_ji(mc, s1, s2)

    elif s2 is None:
        _stats[s1] = {}
        for c, s2 in mc.colours_to_sample_dict().items():
            if s1 != s2:
                _stats[s1][s2] = calc_ji(mc, s1, s2)

    else:
        _stats[s1] = {}
        _stats[s1][s2] = calc_ji(mc, s1, s2)

    return _stats
