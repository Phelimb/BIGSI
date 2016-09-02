#! /usr/bin/env python
from __future__ import print_function
from mcdbg import McDBG
import argparse
import json


def run(parser, args):
    mc = McDBG(ports=args.ports)
    out = mc.bitcount_all()
    print(json.dumps(out))
