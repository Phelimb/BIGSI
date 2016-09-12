#! /usr/bin/env python
from __future__ import print_function
from remcdbg.mcdbg import McDBG
import argparse
import json
import pickle


def run(parser, args, conn_config):
    stats = {}
    mc = McDBG(conn_config=conn_config)
    stats["memory before uncompression (bytes)"] = mc.calculate_memory()
    mc.uncompress_list(sparsity_threshold=args.sparsity_threshold)
    stats["memory after uncompression (bytes)"] = mc.calculate_memory()
    print(json.dumps(stats))
