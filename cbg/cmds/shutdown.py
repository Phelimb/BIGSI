from __future__ import print_function
from cbg.graph import ProbabilisticMultiColourDeBruijnGraph as Graph


def shutdown(conn_config):
    mc = McDBG(conn_config=conn_config)
    return mc.shutdown()
