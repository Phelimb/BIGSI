#! /usr/bin/env python
from __future__ import print_function
from mcdbg import McDBG
import os.path
import logging
import json
logger = logging.getLogger(__name__)


def run(parser, args):
    if args.sample_name is None:
        args.sample_name = os.path.basename(args.kmer_file).split('.')[0]
    with open(args.kmer_file, 'r') as inf:
        kmers = inf.read().splitlines()

    mc = McDBG(ports=args.ports)
    try:
        i = mc.add_sample(args.sample_name)
        mc.set_kmers(kmers, i)
        logger.info("%i\t%i\t%i" %
                    (i, mc.count_kmers(), mc.calculate_memory()))
        print(json.dumps({"result": "success", "colour": i, "kmers": mc.count_kmers(
        ), "memory": mc.calculate_memory()}, indent=4))
    except ValueError as e:
        print(json.dumps({"result": "failed", "message": str(e), "kmers": mc.count_kmers(
        ), "memory": mc.calculate_memory()}, indent=4))
