#! /usr/bin/env python
from __future__ import print_function
from atlasseq.utils import min_lexo
from atlasseq.utils import seq_to_kmers
from atlasseq.mcdbg import McDBG
import argparse
import os.path
import time
from Bio import SeqIO
import json


def per(i):
    return float(sum(i))/len(i)


def parse_input(infile):
    gene_to_kmers = {}
    with open(infile, 'r') as inf:
        for record in SeqIO.parse(inf, 'fasta'):
            gene_to_kmers[record.id] = [
                k for k in seq_to_kmers(str(record.seq))]
    return gene_to_kmers


def query(fasta_file, threshold, conn_config):
    gene_to_kmers = parse_input(fasta_file)
    mc = McDBG(conn_config=conn_config, storage={'probabilistic-redis': {"conn": conn_config,
                                                                         "array_size": 25000000, "num_hashes": 2}})

    colours_to_samples = mc.colours_to_sample_dict()
    results = {}
    found = {}
    for gene, kmers in gene_to_kmers.items():
        found[gene] = {}
        start = time.time()
        found[gene]['results'] = mc.query_kmers(kmers, threshold)
        d = mc.get_kmers_colours(kmers)
        d2 = {}
        for k, v in d.items():
            d2[mc._bytes_to_kmer(k)] = v
        found[gene]['kresults'] = d2
        diff = time.time() - start
        found[gene]['time'] = diff
    print(json.dumps(found, indent=4))
