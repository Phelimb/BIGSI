from __future__ import print_function
from cbg.graph import CBG as Graph


def delete(graph):
    graph.delete_all()
    return {"result": "success"}
