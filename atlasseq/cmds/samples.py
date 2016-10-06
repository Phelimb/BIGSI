#! /usr/bin/env python
from __future__ import print_function
from atlasseq.mcdbg import McDBG
import argparse
import json
import pickle


def run(parser, args, conn_config):
    mc = McDBG(conn_config=conn_config)
    out = mc.colours_to_sample_dict()
    print(json.dumps(out, indent=4))
