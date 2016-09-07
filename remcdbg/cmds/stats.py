#! /usr/bin/env python
from __future__ import print_function
from remcdbg.mcdbg import McDBG
import argparse
import json
import pickle


stats = {}


def run(parser, args, conn_config):
    mc = McDBG(conn_config=conn_config)
    stats["memory (bytes)"] = mc.calculate_memory()
    stats["count_kmers"] = mc.count_kmers() + mc.count_kmers_in_sets()
    stats["samples"] = mc.get_num_colours()

    print(json.dumps(stats, indent=4))
