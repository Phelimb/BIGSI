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
from remcdbg.mcdbg import McDBG

keys = []
N = 10000
with open('scripts/ERR1095101_1000000.txt', 'r') as infile:
    keys.extend(infile.read().splitlines()[:N])

for storage in [{'dict': None}, {'berkeleydb': {'filename': './db'}},
                {"redis": [('localhost', 6379), ('localhost', 6380)]},
                {"probabilistic-inmemory":
                 {"array_size": int(N*10), "num_hashes": 2}},
                {"probabilistic-redis": {"conn": [('localhost', 6379), ('localhost', 6380)],
                                         "array_size": int(N*10), "num_hashes": 2}}]:
    sname = [k for k in storage.keys()][0]
    mc = McDBG(conn_config=[('localhost', 6379)],
               compress_kmers=True, storage=storage)
    mc.delete_all()
    c = 2
    start = time.time()
    for i in range(c):
        mc.add_sample(i)
        mc.insert_kmers(keys, c)
    end = time.time()
    print("insert %s - %i " % (sname, N), mc.calculate_memory(), end-start)
    start = time.time()
    vals = mc.get_kmers_raw(keys)
    end = time.time()
    print("get %s - %i" % (sname, N), end-start)
    start = time.time()
    vals = mc.query_kmers(keys)
    end = time.time()
    print("query %s - %i" % (sname, N), end-start)
