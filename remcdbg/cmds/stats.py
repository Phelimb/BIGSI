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
# a = mc.unique_colour_arrays()
# with open('/data4/projects/atlas/ecol/ana/bitmatrix/sorted_cas.pk', 'wb') as outfile:
#     pickle.dump(a, outfile)
# with open('sorted_cas.pk', 'rb') as infile:
#     print(pickle.load(infile))

# for i, ca in enumerate():
#     if i < 10:
#         print(i, ca)
#     else:
#         break
