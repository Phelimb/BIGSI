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


def search(seq, fasta_file, threshold, graph):
    if fasta_file is not None:
        gene_to_seq = parse_input(fasta_file)
        colours_to_samples = graph.colours_to_sample_dict()
        results = {}
        found = {}
        for gene, seq in gene_to_seq.items():
            found[gene] = {}
            start = time.time()
            found[gene]['results'] = graph.search(seq, threshold=threshold)
            diff = time.time() - start
            found[gene]['time'] = diff
    else:
        logger.debug("Searching %i samples for %s" %
                     (graph.get_num_colours(), seq))
        found = {"seq": graph.search(seq)}

    return json.dumps(found, indent=4)
