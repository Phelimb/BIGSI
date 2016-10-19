#! /usr/bin/env python
from __future__ import print_function
from atlasseq.mcdbg import McDBG
import os.path
import logging
import json
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# from pyseqfile import Reader
from atlasseq.utils import seq_to_kmers


def insert_kmers(mc, kmers, colour, sample):
    # mc.insert_kmers(kmers, colour)
    mc.add_to_kmers_count(kmers, sample)


def insert(kmer_file, conn_config, sample_name=None):
    if sample_name is None:
        sample_name = os.path.basename(kmer_file).split('.')[0]
    logger.info("Inserting kmers from {0} to {1} into database at {2} ".format(
        kmer_file, sample_name, conn_config))

    mc = McDBG(conn_config=conn_config, storage={'probabilistic-redis': {"conn": conn_config,
                                                                         "array_size": 25000000, "num_hashes": 2}})
    try:
        colour = mc.add_sample(sample_name)
        kmers = []
        with open(kmer_file, 'r') as inf:
            for i, line in enumerate(inf):
                read = line.strip()
                for kmer in seq_to_kmers(read):
                    kmers.append(kmer)
                    if i % 100000 == 0 and i > 1:
                        insert_kmers(mc, kmers, colour, sample_name)
                        kmers = []
        insert_kmers(mc, kmers, colour, sample_name)
        print(json.dumps({"result": "success",
                          "colour": colour,
                          "total-kmers": mc.count_kmers(),
                          "kmers-added": mc.count_kmers(sample_name),
                          "memory": mc.calculate_memory()}))
    except ValueError as e:
        print(json.dumps({"result": "failed", "message": str(e),
                          "total-kmers": mc.count_kmers(),
                          "kmers-added": mc.count_kmers(sample_name),
                          "memory": mc.calculate_memory()}))
