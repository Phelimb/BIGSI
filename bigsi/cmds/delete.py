from __future__ import print_function
from bigsi.graph import BIGSI as Graph


def delete(graph):
    graph.delete_all()
    return {"result": "success"}
