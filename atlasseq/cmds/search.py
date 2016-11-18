#! /usr/bin/env python
from __future__ import print_function
# from atlasseq.utils import min_lexo
from atlasseq.utils import seq_to_kmers
from atlasseq.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import argparse
import os.path
import time
from Bio import SeqIO
import json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def per(i):
    return float(sum(i))/len(i)


def parse_input(infile):
    gene_to_kmers = {}
    with open(infile, 'r') as inf:
        for record in SeqIO.parse(inf, 'fasta'):
            gene_to_kmers[record.id] = str(record.seq)
    return gene_to_kmers


def _search(gene_name, seq, results, threshold, graph):
    results[gene_name] = {}
    start = time.time()
    results[gene_name]['results'] = graph.search(seq, threshold=threshold)
    diff = time.time() - start
    results[gene_name]['time'] = diff
    return results


def search(seq, fasta_file, threshold, graph):
    results = {}
    if fasta_file is not None:
        gene_to_seq = parse_input(fasta_file)
        for gene, seq in gene_to_seq.items():
            results = _search(
                gene_name=gene, seq=seq, results=results, threshold=threshold, graph=graph)
    else:
        results = _search(
            gene_name=seq, seq=seq, results=results, threshold=threshold, graph=graph)

    return results
