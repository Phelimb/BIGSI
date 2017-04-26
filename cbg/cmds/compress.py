#! /usr/bin/env python
from __future__ import print_function
from cbg.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import argparse
import json
import pickle


def compress(conn_config):
    stats = {}
    mc = McDBG(conn_config=conn_config)
    stats["memory before compression (bytes)"] = mc.calculate_memory()
    # mc.compress_list(sparsity_threshold=args.sparsity_threshold)
    # mc.compress_hash()  # (sparsity_threshold=args.sparsity_threshold)
    mc.compress()  # (sparsity_threshold=args.sparsity_threshold)

    stats["memory after compression (bytes)"] = mc.calculate_memory()
    print(json.dumps(stats))
