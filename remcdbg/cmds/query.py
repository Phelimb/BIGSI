#! /usr/bin/env python
from __future__ import print_function
from remcdbg.utils import min_lexo
from remcdbg.utils import seq_to_kmers
from remcdbg.mcdbg import McDBG
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
                min_lexo(k) for k in seq_to_kmers(str(record.seq)) if not "N" in k and not "Y" in k]
    return gene_to_kmers


def run(parser, args, conn_config):
    gene_to_kmers = parse_input(args.fasta)
    mc = McDBG(conn_config=conn_config)
    colours_to_samples = mc.colours_to_sample_dict()
    results = {}
    found = {}
    for gene, kmers in gene_to_kmers.items():
        found[gene] = {}
        found[gene]['samples'] = []
        results[gene] = []
        start = time.time()
        _found = mc.query_kmers_100_per(kmers)
        for i, p in enumerate(_found):
            if p == 1:
                found[gene]['samples'].append(
                    colours_to_samples.get(i, 'missing'))
        diff = time.time() - start
        found[gene]['time'] = diff
    print(json.dumps(found, indent=4))

    # ## Perf tests ###
    # found = {}
    # for k in kmers:
    #     print(k)
    # for gene, kmers in gene_to_kmers.items():

    #     found[gene] = {}
    #     found[gene]['100%'] = {}
    #     found[gene]['100%']['samples'] = []
    #     found[gene]['90%'] = {}
    #     found[gene]['90%']['samples'] = []
    #     found[gene]['90%_getbit'] = {}
    #     found[gene]['90%_getbit']['samples'] = []

    #     results[gene] = []
    #     # getbit
    #     start = time.time()
    #     for i, res in enumerate(mlc.query_kmers_colours(kmers)):
    #         results[gene].append(res)
    #     percent_kmers = list(map(per, zip(*results[gene][1:])))
    #     colours_indexes = results[gene][0]
    #     print(percent_kmers)
    #     for i, p in enumerate(percent_kmers):
    #         if p > 0.9:
    #             found[gene]['90%_getbit']['samples'].append(
    #                 colours_to_samples.get(colours_indexes[i], 'missing'))
    #     diff = time.time() - start
    #     found[gene]['90%_getbit']['time'] = diff
    #     # get + restricted sum
    #     start = time.time()
    #     colours = mc.get_non_0_kmer_colours(kmers)
    #     for i, res in enumerate(mc.query_kmers(kmers)):
    #         results[gene].append(res)
    #     columns = [
    #         j for i, j in enumerate(zip(*results[gene])) if i in colours]
    #     percent_kmers = list(map(per, columns))

    #     colours_indexes = results[gene][0]
    #     for i, p in enumerate(percent_kmers):
    #         if p > 0.9:
    #             found[gene]['90%']['samples'].append(
    #                 colours_to_samples.get(colours_indexes[i], 'missing'))
    #     diff = time.time() - start
    #     found[gene]['90%']['time'] = diff
    #     # bitor method
    #     start = time.time()
    #     _found = mc.query_kmers_100_per(kmers)
    #     for i, p in enumerate(_found):
    #         if p > 0.9:
    #             found[gene]['100%']['samples'].append(
    #                 colours_to_samples.get(i, 'missing'))
    #     diff = time.time() - start
    #     found[gene]['100%']['time'] = diff

    # print(found)
    # # with open('%s.json' % args.fasta, 'w') as outfile:
