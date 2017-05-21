from __future__ import print_function
from cbg.graph import ProbabilisticMultiColourDeBruijnGraph as Graph


def delete(graph):
    graph.delete_all()
    return {"result": "success"}
