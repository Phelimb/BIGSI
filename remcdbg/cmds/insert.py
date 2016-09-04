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
        i = mc.add_sample(args.sample_name)
        with open(args.kmer_file, 'r') as inf:
            kmers = inf.read().splitlines()
        for kmer in kmers:
            mc.add_kmer(kmer, i)
        logger.info("%i\t%i\t%i" %
                    (i, mc.count_kmers(), mc.calculate_memory()))
        print(json.dumps({"result": "success", "colour": i, "kmers": mc.count_kmers(
        ), "memory": mc.calculate_memory()}))
    except ValueError as e:
        print(json.dumps({"result": "failed", "message": str(e), "kmers": mc.count_kmers(
        ), "memory": mc.calculate_memory()}))
