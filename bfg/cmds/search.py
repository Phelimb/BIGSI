#! /usr/bin/env python
from __future__ import print_function
# from bfg.utils import min_lexo
from bfg.utils import seq_to_kmers
from bfg.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import argparse
import os.path
import time
from Bio import SeqIO
import json
import logging

logger = logging.getLogger(__name__)
from bfg.utils import DEFAULT_LOGGING_LEVEL
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


def _search(gene_name, seq, results, threshold, graph, output_format="json", pipe=False):
    if pipe:
        if output_format == "tsv":
            start = time.time()
            result = graph.search(seq, threshold=threshold)
            diff = time.time() - start
            if result:
                for sample_id, percent in result.items():
                    print(
                        "\t".join([gene_name, sample_id, str(percent), str(diff)]))
            else:
                print("\t".join([gene_name, "NA", str(0), str(diff)]))
        elif output_format == "fasta":
            samples = graph.sample_to_colour_lookup.keys()
            print(" ".join(['>', gene_name]))
            print(seq)
            kmer_presence = graph.lookup(seq)
            for sample in samples:
                print(
                    " ".join(['>', gene_name, sample, "kmer-%i coverage" % graph.kmer_size]))
                presence = []
                for kmer in seq_to_kmers(seq):
                    if sample in kmer_presence.get(kmer, []):
                        presence.append("1")
                    else:
                        presence.append("0")
                print("".join(presence))
        else:
            result = {}
            start = time.time()
            result['results'] = graph.search(seq, threshold=threshold)
            diff = time.time() - start
            result['time'] = diff
            print(json.dumps({gene_name: result}))
    else:
        results[gene_name] = {}
        start = time.time()
        results[gene_name]['results'] = graph.search(seq, threshold=threshold)
        diff = time.time() - start
        results[gene_name]['time'] = diff
    return json.dumps(results)


def search(seq, fasta_file, threshold, graph, output_format="json", pipe=False):
    if output_format == "tsv":
        print("\t".join(
            ["gene_name", "sample_id", str("kmer_coverage_percent"), str("time")]))
    results = {}
    if fasta_file is not None:
        for gene, seq in parse_input(fasta_file):
            results = _search(
                gene_name=gene, seq=seq, results=results, threshold=threshold,
                graph=graph, output_format=output_format, pipe=pipe)
    else:
        results = _search(
            gene_name=seq, seq=seq, results=results, threshold=threshold,
            graph=graph, output_format=output_format, pipe=pipe)
    return results
