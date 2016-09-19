#! /usr/bin/env python
from __future__ import print_function
from remcdbg.mcdbg import McDBG
import argparse
import json
import pickle
import collections


stats = {}


def run(parser, args, conn_config):
    mc = McDBG(conn_config=conn_config, storage={'redis': conn_config})
    stats["memory (bytes)"] = mc.calculate_memory()
    stats["keys"] = mc.count_keys()
    stats["kmers"] = mc.count_kmers()
    stats["samples"] = mc.get_num_colours()

    print(json.dumps(collections.OrderedDict(sorted(stats.items())), indent=4))
