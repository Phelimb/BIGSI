#! /usr/bin/env python
from __future__ import print_function
from mcdbg import McDBG
import argparse
import os.path
parser = argparse.ArgumentParser()
parser.add_argument("kmer_file")
parser.add_argument("--sample_name", required=False)
parser.add_argument("--ports", type=int, nargs='+')
args = parser.parse_args()

if args.sample_name is None:
    args.sample_name = os.path.basename(args.kmer_file).split('.')[0]
with open(args.kmer_file, 'r') as inf:
    kmers = inf.read().splitlines()

mc = McDBG(ports=args.ports)
# mc.delete()

i = mc.add_sample(args.sample_name)
mc.set_kmers(kmers, i)
print(i, mc.count_kmers(), mc.calculate_memory())
