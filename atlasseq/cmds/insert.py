#! /usr/bin/env python
from __future__ import print_function
from atlasseq.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import os.path
import logging
import json
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# from pyseqfile import Reader
from atlasseq.utils import seq_to_kmers


def insert_kmers(mc, kmers, colour, sample, count_only=False):
    if not count_only:
        mc.insert_kmers(kmers, colour)
    mc.add_to_kmers_count(kmers, sample)


def load_all_kmers(f):
    kmers = []
    with open(f, 'r') as inf:
        for line in inf:
            read = line.strip()
            for kmer in seq_to_kmers(read):
                kmers.append(kmer)
    return kmers


def insert_colour(mc, kmer_file, colour, sample_name, count_only, intersect_kmers=None):
    kmers = []
    with open(kmer_file, 'r') as inf:
        for i, line in enumerate(inf):
            read = line.strip()
            for kmer in seq_to_kmers(read):
                kmers.append(kmer)
                if i % 100000 == 0 and i > 1:
                    if intersect_kmers is not None:
                        kmers = list(set(kmers) & intersect_kmers)
                    insert_kmers(
                        mc, kmers, colour, sample_name, count_only=count_only)
                    kmers = []
    if intersect_kmers is not None:
        kmers = list(set(kmers) & intersect_kmers)
    insert_kmers(mc, kmers, colour, sample_name, count_only=count_only)


def insert(kmer_file, conn_config, force=False, sample_name=None, intersect_kmers_file=None, count_only=False):
    if sample_name is None:
        sample_name = os.path.basename(kmer_file).split('.')[0]
    logger.info("Inserting kmers from {0} to {1} into database at {2} ".format(
        kmer_file, sample_name, conn_config))
    if intersect_kmers_file is not None:
        intersect_kmers = set(load_all_kmers(intersect_kmers_file))
    else:
        intersect_kmers = None

    mc = Graph(storage={'probabilistic-redis': {"conn": conn_config,
                                                "array_size": 25000000, "num_hashes": 2}})
    try:
        colour = mc.add_sample(sample_name)
        insert_colour(
            mc, kmer_file, colour, sample_name, count_only, intersect_kmers)
        print(json.dumps({"result": "success",
                          "colour": colour,
                          "total-kmers": mc.count_kmers(),
                          "kmers-added": mc.count_kmers(sample_name),
                          "memory": mc.calculate_memory()}))
    except ValueError as e:
        if not force:
            print(json.dumps({"result": "failed", "message": str(e),
                              "total-kmers": mc.count_kmers(),
                              "kmers-added": mc.count_kmers(sample_name),
                              "memory": mc.calculate_memory()}))
        else:
            colour = mc.get_sample_colour(sample_name)
            insert_colour(
                mc, kmer_file, colour, sample_name, count_only, intersect_kmers)
            print(json.dumps({"result": "success",
                              "colour": colour,
                              "total-kmers": mc.count_kmers(),
                              "kmers-added": mc.count_kmers(sample_name),
                              "memory": mc.calculate_memory()}))
