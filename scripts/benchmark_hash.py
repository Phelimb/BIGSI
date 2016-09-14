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
# with open('/data4/projects/atlas/ecol/data/kmers/k31/ERR1095101.txt',
# 'r') as infile:
# with open('scripts/ERR1095101_100.txt', 'r') as infile:
with open('scripts/ERR1095101_1000000.txt', 'r') as infile:
    keys.extend(infile.read().splitlines()[:1000000])

start = time.time()

mc = McDBG(conn_config=[('localhost', 6379)], compress_kmers=True)
mc.flushall()
start = time.time()
c = 100  # random.randint(0, 10000)
for i in range(c+1):
    mc.add_sample(i)
mc.set_kmers(keys, c)
mc.num_colours = mc.get_num_colours()

end = time.time()
print("bitarray N=50000", mc.calculate_memory())


start = time.time()
mc = McDBG(conn_config=[('localhost', 6379)], compress_kmers=True)
start = time.time()
print(mc.count_kmers(), mc.count_kmers_in_lists())
mc.compress_hash()
print("Compress hash N=50000", mc.calculate_memory())

end = time.time()
