from __future__ import print_function
from atlasseq.graph import ProbabilisticMultiColourDeBruijnGraph as Graph


def delete(conn_config):
    mc = Graph(storage={'redis-cluster': {"conn": conn_config,
                                          "array_size": 25000000,
                                          "num_hashes": 2}})
    mc.delete_all()
    return {"result": "success"}
