from __future__ import print_function
from mcdbg import McDBG


def run(parser, args):
    mc = McDBG(ports=args.ports)
    return mc.shutdown()
