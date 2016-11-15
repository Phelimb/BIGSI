#! /usr/bin/env python
from __future__ import print_function
from atlasseq.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import argparse
import json
import pickle


def dumps(graph):
    return graph.dumps()
