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
with open('scripts/ERR1095101_100.txt', 'r') as infile:
    keys.extend(infile.read().splitlines())

# start = time.time()
# mc = McDBG(conn_config=[('localhost', 6379)], compress_kmers=True)
# mc.delete()
# start = time.time()
# mc.set_kmers(keys, 5000)
# # print(" INFO memory")
# # print(" DBSIZE")
# end = time.time()
# print("bitarray N=5000", mc.sample_redis.info(
#     'memory').get('used_memory_human'))


# start = time.time()
# mc = McDBG(conn_config=[('localhost', 6379)], compress_kmers=True)
# mc.delete()
# start = time.time()
# mc.add_kmers_to_list(keys, 5000)
# # print(" INFO memory")
# # print(" DBSIZE")
# end = time.time()
# print("List N=5000", mc.sample_redis.info('memory').get('used_memory_human'))


# start = time.time()
# mc = McDBG(conn_config=[('localhost', 6379)], compress_kmers=True)
# mc.delete()
# start = time.time()
# mc.set_kmers(keys, 10000)
# mc.set_kmers(keys, 5000)
# # print(" INFO memory")
# # print(" DBSIZE")
# end = time.time()
# print("bitarray N=10000 X=2", mc.sample_redis.info(
#     'memory').get('used_memory_human'))

# # print(mc.sample_redis.info('memory').get('used_memory_human'))


# start = time.time()
# mc = McDBG(conn_config=[('localhost', 6379)], compress_kmers=True)
# mc.delete()
# start = time.time()
# mc.add_kmers_to_list(keys, 5000)
# mc.add_kmers_to_list(keys, 10000)
# # print(" INFO memory")
# # print(" DBSIZE")
# end = time.time()
# print("list N=10000 X=2", mc.sample_redis.info(
#     'memory').get('used_memory_human'))


# start = time.time()
# mc = McDBG(conn_config=[('localhost', 6379)], compress_kmers=True)
# mc.delete()
# start = time.time()
# mc.set_kmers(keys, 2000)
# mc.set_kmers(keys, 4000)
# mc.set_kmers(keys, 5000)
# mc.set_kmers(keys, 10000)
# end = time.time()
# print("bitarray N=10000 X=4", mc.sample_redis.info(
#     'memory').get('used_memory_human'))


# start = time.time()
# mc = McDBG(conn_config=[('localhost', 6379)], compress_kmers=True)
# mc.delete()
# start = time.time()
# mc.add_kmers_to_list(keys, 2000)
# mc.add_kmers_to_list(keys, 4000)
# mc.add_kmers_to_list(keys, 5000)
# mc.add_kmers_to_list(keys, 10000)
# # print(" INFO memory")
# # print(" DBSIZE")
# end = time.time()
# print("list N=10000 X=4", mc.sample_redis.info(
#     'memory').get('used_memory_human'))


def run_set(x):
    start = time.time()
    mc = McDBG(conn_config=[('localhost', 6379)], compress_kmers=True)
    mc.delete()
    start = time.time()
    for _ in range(x):
        mc.set_kmers(keys, random.randint(0, 10000))
    end = time.time()
    print("bitarray N=10000 sparsity=%i%%" % int(100*float(x)/10000), mc.sample_redis.info(
        'memory').get('used_memory_human'))


def run_list(x):
    start = time.time()
    mc = McDBG(conn_config=[('localhost', 6379)], compress_kmers=True)
    mc.delete()
    start = time.time()
    for _ in range(x):
        mc.add_kmers_to_list(keys, random.randint(0, 10000))
    end = time.time()
    print("list N=10000 sparsity=%i%%" % int(100*float(x)/10000), mc.sample_redis.info(
        'memory').get('used_memory_human'))

for x in [100, 250, 500, 1000, 5000]:
    run_set(x)
    run_list(x)
