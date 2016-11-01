from __future__ import print_function
from atlasseq import hash_key
from atlasseq.bytearray import ByteArray
import hashlib
from bitstring import BitArray
from redispartition import RedisCluster
from bitarray import bitarray
import math
import os
import json
from sys import getsizeof
import sys
from HLL import HyperLogLog
try:
    import redis
except ImportError:
    redis = None

try:
    import bsddb3 as bsddb
except ImportError:
    bsddb = None

try:
    import leveldb
except ImportError:
    leveldb = None


def byte_to_bitstring(byte):
    a = str("{0:b}".format(byte))
    if len(a) < 8:
        a = "".join(['0'*(8-len(a)), a])
    return a


def setbit(bytes, i):
    a = bitarray()
    a.frombytes(bytes)
    try:
        a[i] = 1
        return a.tobytes()
    except IndexError:
        a = bitarray()
        _bytes = b"".join(
            [bytes, b'\x00']*math.ceil(float(1+i-len(bytes)*8)/8))
        return setbit(_bytes, i)


def indexes(bitarray):
    indexes = []
    i = 0
    while True:
        try:
            i = bitarray.index(True, i)
            indexes.append(i)
            i += 1
        except ValueError:
            break
    return indexes


class BerkeleyDBStorage(BaseStorage):

    def __init__(self, config):
        if 'filename' not in config:
            raise ValueError(
                "You must supply a 'filename' in your config%s" % config)
        self.db_file = config['filename']
        try:
            self.storage = bsddb.hashopen(self.db_file)
        except AttributeError:
            raise ValueError(
                "Please install bsddb3 to use berkeley DB storage")

    def __exit__(self, type, value, traceback):
        self.storage.sync()

    def keys(self):
        return self.storage.keys()

    def count_keys(self):
        return len(self.keys())

    def __setitem__(self, key, val):
        if isinstance(key, str):
            key = str.encode(key)
        self.storage[key] = val

    def __getitem__(self, key):
        if isinstance(key, str):
            key = str.encode(key)
        return self.storage[key]

    def get(self, key, default=None):
        if isinstance(key, str):
            key = str.encode(key)
        try:
            return self[key]
        except KeyError:
            return default

    def delete_all(self):
        self.storage.close()
        os.remove(self.db_file)
        self.storage = bsddb.hashopen(self.db_file)

    def getmemoryusage(self):
        return 0
