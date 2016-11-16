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
        graph.insert_kmers(kmers, colour)
    graph.add_to_kmers_count(kmers, sample)


def load_all_kmers(f):
    kmers = []
    with open(f, 'r') as inf:
        for line in inf:
            read = line.strip()
            for kmer in seq_to_kmers(read):
                kmers.append(kmer)
    return set(kmers)


# def extract_kmers(kmer_file):
#     kmers = set()
#     with open(kmer_file, 'r') as inf:
#         for i, line in enumerate(inf):
#             read = line.strip()
#             for kmer in seq_to_kmers(read):
#                 kmers.add(kmer)
#     if intersect_kmers is not None:
#         kmers = list(set(kmers) & intersect_kmers)
#     return kmers

def insert(kmers, kmer_file, graph, force=False, sample_name=None, intersect_kmers_file=None, count_only=False):
    if sample_name is None:
        sample_name = os.path.basename(kmer_file).split('.')[0]

    if intersect_kmers_file is not None:
        intersect_kmers = set(load_all_kmers(intersect_kmers_file))
    else:
        intersect_kmers = None

    if kmer_file is not None:
        kmers = list(load_all_kmers(kmer_file))

    logger.debug("Loaded %i kmers" % len(kmers))
    try:
        graph.insert(kmers, sample_name)
        return {"result": "success",
                "colour": graph.get_colour_from_sample(sample_name),
                #                          "total-kmers": graph.count_kmers(),
                #                          "kmers-added": graph.count_kmers(sample_name),
                #                          "memory": graph.calculate_memory()
                }
    except ValueError as e:
        if not force:
            return {"result": "failed", "message": str(e),
                    # "total-kmers": graph.count_kmers(),
                    # "kmers-added": graph.count_kmers(sample_name),
                    # "memory": graph.calculate_memory()
                    }
        else:
            raise NotImplemented("Force not implemented yet")
