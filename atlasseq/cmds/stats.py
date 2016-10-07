#! /usr/bin/env python
from __future__ import print_function
from atlasseq.mcdbg import McDBG
import argparse
import json
import pickle
import collections


_stats = {}


def stats(conn_config):
    mc = McDBG(conn_config=conn_config, storage={'redis': conn_config})
    _stats["memory (bytes)"] = mc.calculate_memory()
    _stats["keys"] = mc.count_keys()
    _stats["kmers"] = mc.count_kmers()
    _stats["samples"] = mc.get_num_colours()

    return _stats  # collections.OrderedDict(sorted(.items()))
