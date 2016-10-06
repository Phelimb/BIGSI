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
from atlasseq.mcdbg import McDBG

keys = []
# with open('/data4/projects/atlas/ecol/data/kmers/k31/ERR1095101.txt',
# 'r') as infile:
# with open('scripts/ERR1095101_100.txt', 'r') as infile:
with open('scripts/ERR1095101_1000000.txt', 'r') as infile:
    keys.extend(infile.read().splitlines()[:100000])

start = time.time()

mc = McDBG(conn_config=[('localhost', 6379)], compress_kmers=True)
mc.flushall()
c = 2  # random.randint(0, 10000)
start = time.time()
for i in range(c):
    mc.add_sample(i)
    mc.insert_kmers(keys, c)
    mc.insert_kmers(keys, c+1)


mc.num_colours = mc.get_num_colours()
print(mc.count_kmers())
end = time.time()
print("hashset N=50000", mc.calculate_memory(), end-start)


end = time.time()

mc.flushall()
start = time.time()
for i in range(c):
    mc.add_sample(i)
    mc.set_kmers(keys, c)
    mc.set_kmers(keys, c+1)

mc.num_colours = mc.get_num_colours()

end = time.time()
print("bitarray N=50000", mc.calculate_memory(), end-start)
