#! /usr/bin/env python
from __future__ import print_function
from mcdbg import McDBG
import argparse
import os.path

parser = argparse.ArgumentParser()
parser.add_argument("fasta")
parser.add_argument("--ports", type=int, nargs='+')
args = parser.parse_args()


def fasta_to_kmers(fasta):
    kmers = []
    seq = ""
    with open(fasta, 'r') as infile:
        for i, line in enumerate(infile.read().splitlines()):
            if line[0] == ">":
                seq = ""
            else:
                seq = "".join([seq, line])
    return [seq[i:i+63] for i in range(len(seq)-63+1)]


kmers = fasta_to_kmers(args.fasta)
mc = McDBG(ports=args.ports)
print(len(kmers))
for res in mc.query_kmers(kmers):
    res
