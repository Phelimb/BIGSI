#! /usr/bin/env python
import time
import sys
import os
sys.path.append(
    os.path.realpath(
        os.path.join(
            os.path.dirname(__file__),
            "..")))
from remcdbg.mcdbg import McDBG

keys = []
# with open('/data4/projects/atlas/ecol/data/kmers/k31/ERR1095101.txt',
# 'r') as infile:
with open('scripts/ERR1095101_100.txt', 'r') as infile:
    keys.extend(infile.read().splitlines())

mc = McDBG(ports=['6379'])
mc.delete()
start = time.time()
mc.set_kmers(keys, 1)
end = time.time()
print(mc.sample_redis.execute_command('info', 'memory'))
print 'NON compress performed in {0} seconds'.format(end - start)


mc = McDBG(ports=['6379'], compress_kmers=True)
mc.delete()
mcstart = time.time()
mc.set_kmers(keys, 1)

end = time.time()
print(mc.sample_redis.execute_command('info', 'memory'))
print 'compress performed in {0} seconds'.format(end - start)
for i, k in enumerate(mc.kmers()):
    if i > 5:
        break
    print(k)
