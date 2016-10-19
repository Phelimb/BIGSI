#! /usr/bin/env python
from __future__ import print_function
from atlasseq.mcdbg import McDBG
import argparse
import json
import pickle
import collections


_stats = {}


def jaccard_index(s1, s2, conn_config):
    mc = McDBG(conn_config=conn_config, storage={'probabilistic-redis': {"conn": conn_config,
                                                                         "array_size": 25000000, "num_hashes": 2}})
    _stats["jaccard-index"] = mc.jaccard_index(s1, s2)
    _stats["jaccard-distance"] = mc.jaccard_distance(s1, s2)
    _stats["symmetric_difference"] = mc.symmetric_difference(s1, s2)
    _stats["num-kmers-unique-to-%s" % s1] = mc.difference(s1, s2)
    _stats["num-kmers-unique-to-%s" % s2] = mc.difference(s2, s1)
    _stats["sample-1"] = s1
    _stats["sample-2"] = s2
    return _stats
