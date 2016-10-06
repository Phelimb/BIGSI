from __future__ import print_function
from atlasseq.mcdbg import McDBG


def run(parser, args, conn_config):
    mc = McDBG(conn_config=conn_config)
    return mc.shutdown()
