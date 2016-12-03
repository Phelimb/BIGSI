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
from atlasseq.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)


def per(i):
    return float(sum(i))/len(i)


def parse_input(infile):
    gene_to_kmers = {}
    with open(infile, 'r') as inf:
        for record in SeqIO.parse(inf, 'fasta'):
            gene_to_kmers[record.id] = str(record.seq)
            yield (record.id, str(record.seq))
    # return gene_to_kmers


def _search(gene_name, seq, results, threshold, graph, output_format="json"):
    results[gene_name] = {}
    start = time.time()
    results[gene_name]['results'] = graph.search(seq, threshold=threshold)
    diff = time.time() - start
    results[gene_name]['time'] = diff
    if output_format == "tsv":
        if results[gene_name]['results']:
            for sample_id, percent in results[gene_name]['results'].items():
                print(
                    "\t".join([gene_name, sample_id, str(percent), str(diff)]))
        else:
            print("\t".join([gene_name, "NA", str(0), str(diff)]))
    else:
        print(json.dumps({gene_name: results[gene_name]}))
    return results


def search(seq, fasta_file, threshold, graph, output_format="json"):
    if output_format == "tsv":
        print("\t".join(
            ["gene_name", "sample_id", str("kmer_coverage_percent"), str("time")]))
    results = {}
    if fasta_file is not None:
        for gene, seq in parse_input(fasta_file):
            results = _search(
                gene_name=gene, seq=seq, results=results, threshold=threshold, graph=graph, output_format=output_format)
    else:
        results = _search(
            gene_name=seq, seq=seq, results=results, threshold=threshold, graph=graph, output_format=output_format)
    return results
