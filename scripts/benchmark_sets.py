#! /usr/bin/env python
import time
import sys
import os
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
from bfg.mcdbg import McDBG

keys = []
# with open('/data4/projects/atlas/ecol/data/kmers/k31/ERR1095101.txt',
# 'r') as infile:
with open('scripts/ERR1095101_1000000.txt', 'r') as infile:
    keys.extend(infile.read().splitlines())

start = time.time()
mc = McDBG(ports=['6379'], compress_kmers=True)
mc.delete()
start = time.time()
mc.set_kmers(keys, 10000)
# print(" INFO memory")
# print(" DBSIZE")
end = time.time()
print(mc.sample_redis.info('memory').get('used_memory_human'))
print(mc.count_kmers())
print(end - start)

start = time.time()
mc = McDBG(ports=['6379'], compress_kmers=True)
mc.delete()
start = time.time()
mc.add_kmers_to_set(keys, 10000)
# print(" INFO memory")
# print(" DBSIZE")
end = time.time()
print(mc.sample_redis.info('memory').get('used_memory_human'))
print(mc.count_kmers())
print(end - start)

# print 'compress performed in {0} seconds'.format(end - start)
# for i, k in enumerate(mc.kmers()):
#     if i > 5:
#         break
#     print(k)
