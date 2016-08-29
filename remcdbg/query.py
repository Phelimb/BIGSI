#! /usr/bin/env python
from __future__ import print_function
from mcdbg import McDBG
import argparse
import os.path
import time
from Bio import SeqIO
import json
import numpy as np
# np.set_printoptions(threshold=np.inf)

parser = argparse.ArgumentParser()
parser.add_argument("fasta")
parser.add_argument("--ports", type=int, nargs='+')
args = parser.parse_args()


def seq_to_kmers(seq):
    for i in range(len(seq)-31+1):
        yield seq[i:i+31]


def per(i):
    return float(sum(i))/len(i)

gene_to_kmers = {}
with open(args.fasta, 'r') as inf:
    for record in SeqIO.parse(inf, 'fasta'):
        gene_to_kmers[record.id] = [
            str(k) for k in seq_to_kmers(record.seq) if not "N" in k and not "Y" in k]
# print(gene_to_kmers.keys())
# print(len(gene_to_kmers.values()[0]))
mc = McDBG(ports=args.ports)
colours_to_samples = mc.colours_to_sample_dict()
# print(colours_to_samples)
results = {}

found = {}
for gene, kmers in gene_to_kmers.items():
    found[gene] = {}
    found[gene]['90%'] = {}
    found[gene]['100%'] = {}
    found[gene]['90%']['samples'] = []
    found[gene]['100%']['samples'] = []

    results[gene] = []

    start = time.time()
    for i, res in enumerate(mc.query_kmers(kmers)):
        results[gene].append(res)
    percent_kmers = list(map(per, zip(*results[gene])))
    for i, p in enumerate(percent_kmers):
        if p > 0.9:
            found[gene]['90%']['samples'].append(
                colours_to_samples.get(i, 'missing'))
    diff = time.time() - start
    found[gene]['90%']['time'] = diff
    # bitor method
    start = time.time()
    _found = mc.query_kmers_100_per(kmers)
    for i, p in enumerate(_found):
        if p > 0.9:
            found[gene]['100%']['samples'].append(
                colours_to_samples.get(i, 'missing'))
    diff = time.time() - start
    found[gene]['100%']['time'] = diff


with open('%s.json' % args.fasta, 'w') as outfile:
    json.dump(found, outfile)
