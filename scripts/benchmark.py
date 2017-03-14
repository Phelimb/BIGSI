#! /usr/bin/env python
import time
import sys
import os
import random
sys.path.append(
    os.path.realpath(
        os.path.join(
            os.path.dirname(__file__),
            "..")))
sys.path.append(
    os.path.realpath(
        os.path.join(
            os.path.dirname(__file__),
            "../redis-py")))
from bfg import ProbabilisticMultiColourDeBruijnGraph as Graph


keys = []
N = 100000
with open('bfg/tests/data/test_kmers.txt', 'r') as infile:
    keys.extend(infile.read().splitlines()[:N]*1000)
N = len(keys)
for storage in [
    #{'dict': None},
    {'berkeleydb': {'filename': './db'}},
    # {"redis": {
    #     "conn": [('localhost', 6379, 2)]}},
    # {"redis-cluster": {
    #     "conn": [('localhost', 7000, 0)], 'credis':True}},
    # {"redis-cluster": {
    #     "conn": [('localhost', 7000, 0)], 'credis':False}}
]:
    sname = [k for k in storage.keys()][0]
    mc = Graph(storage=storage, bloom_filter_size=10000)
    mc.delete_all()
    c = 3
    start = time.time()
    bfs = []
    for i in range(c):
        bf = mc.bloom(keys)
        bfs.append(bf)
    mc.build(bfs, range(c))
    end = time.time()
    print("insert %s - %i %i colours" % (sname, N, c), end-start)
    start = time.time()
    vals = mc._search(keys[:10000], threshold=.8)
    end = time.time()
    print("get kmers %s - %i" % (sname, N), end-start)
    start = time.time()
    vals = mc.dump('/tmp/tmp_dump')
    end = time.time()
    print("dump kmers %s - %i" % (sname, N), end-start)
    # start = time.time()
    # vals = mc.search(keys[0])
    # end = time.time()
    # print("query %s - %i" % (sname, N), end-start)
