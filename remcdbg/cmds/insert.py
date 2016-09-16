#! /usr/bin/env python
from __future__ import print_function
from remcdbg.mcdbg import McDBG
import os.path
import logging
import json
logger = logging.getLogger(__name__)


def run(parser, args, conn_config):
    if args.sample_name is None:
        args.sample_name = os.path.basename(args.kmer_file).split('.')[0]
    mc = McDBG(conn_config=conn_config)
    try:
        colour = mc.add_sample(args.sample_name)
        kmers = []
        with open(args.kmer_file, 'r') as inf:
            kmers.extend(inf.read().splitlines())
            mc.insert_kmers(kmers, colour)

        #     kmers = []
        #     for i, line in enumerate(inf):
        #         kmer = line.strip()
        #         kmers.append(kmer)
        #         if i % 1000000 == 0 and i > 1:
        #             mc.insert_kmers(kmers, colour)
        #             kmers = []
        # mc.insert_kmers(kmers, colour)
        ckmers = []
        for i, kmer in enumerate(kmers):
            ckmers.append(kmer)
            if i % 100000 == 0 and i > 1:
                mc.clusters['stats'].pfadd('kmer_count', *ckmers)
                ckmers = []

        # kmers = inf.read().splitlines()

        print(json.dumps({"result": "success", "colour": colour, "kmers": mc.count_kmers(
        ), "memory": mc.calculate_memory()}))
    except ValueError as e:
        print(json.dumps({"result": "failed", "message": str(e), "kmers": mc.count_kmers(
        ), "memory": mc.calculate_memory()}))
