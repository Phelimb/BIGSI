#! /usr/bin/env python
from __future__ import print_function
from atlasseq.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import os.path
import logging
import json
logger = logging.getLogger(__name__)
from atlasseq.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)

from pyseqfile import Reader
from atlasseq.utils import seq_to_kmers
from atlasseq.utils import kmer_reader


def insert_kmers(mc, kmers, colour, sample, count_only=False):
    if not count_only:
        graph.insert_kmers(kmers, colour)
    graph.add_to_kmers_count(kmers, sample)


def insert(kmers, kmer_file, graph, force=False, sample_name=None, intersect_kmers_file=None, sketch_only=False):
    if sample_name is None:
        sample_name = os.path.basename(kmer_file).split('.')[0]

    if intersect_kmers_file is not None:
        intersect_kmers = set(load_all_kmers(intersect_kmers_file))
    else:
        intersect_kmers = None

    if kmer_file is not None:
        kmers = kmer_reader(kmer_file)

    logger.debug("Starting insert. ")
    try:
        graph.insert(kmers, sample_name, sketch_only=sketch_only)
        return {"result": "success",
                "colour": graph.get_colour_from_sample(sample_name),
                "total-kmers": graph.count_kmers(),
                "kmers-added": graph.count_kmers(sample_name),
                #                          "memory": graph.calculate_memory()
                }
    except ValueError as e:
        if not force:
            return {"result": "failed", "message": str(e),
                    "total-kmers": graph.count_kmers(),
                    "kmers-added": graph.count_kmers(sample_name),
                    # "memory": graph.calculate_memory()
                    }
        else:
            raise NotImplemented("Force not implemented yet")
