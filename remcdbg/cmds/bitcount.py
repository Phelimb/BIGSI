#! /usr/bin/env python
from __future__ import print_function
from remcdbg.mcdbg import McDBG
import argparse
import json


def run(parser, args, conn_config):
    mc = McDBG(conn_config=conn_config)
    out = mc.bitcount_all()
    print(json.dumps(out))
