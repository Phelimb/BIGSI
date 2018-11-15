import redis
import sys
import os

# from rediscluster import StrictRedisCluster
from bigsi.utils import hash_key
from bigsi.utils import chunks
from bigsi.bitvector import BitArray
import shutil
import logging
import time
import crc16
import math
import struct

# from redis_protocol import encode as redis_encode
# from redis.connection import Connection
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)
from bigsi.utils import DEFAULT_LOGGING_LEVEL

logger.setLevel(DEFAULT_LOGGING_LEVEL)

try:
    import bsddb3 as bsddb
except ImportError:
    bsddb = None


from credis import Connection


class BaseStorage(object):
    def convert_key(self, key):
        return key.encode("utf-8")

    def __setitem__(self, key, val):
        key = self.convert_key(key)
        self.storage[key] = val

    def __getitem__(self, key):
        key = self.convert_key(key)
        return self.storage[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def set_bit(self):
        raise NotImplementedError("Implemented in subclass")

    def get_bit(self):
        raise NotImplementedError("Implemented in subclass")

    def __convert_to_integer_key(self, key):
        return key + ":int"

    def __convert_to_string_key(self, key):
        return key + ":string"

    def __convert_to_bitarray_key(self, key):
        return key + ":bitarray"

    def set_integer(self, key, value):
        key = self.__convert_to_integer_key(key)
        self[key] = struct.pack("Q", int(value))

    def get_integer(self, key):
        key = self.__convert_to_integer_key(key)
        return struct.unpack("Q", self[key])[0]

    def set_bitarray(self):
        raise NotImplementedError("Implemented in subclass")

    def get_bitarray(self):
        raise NotImplementedError("Implemented in subclass")

    def set_string(self):
        raise NotImplementedError("Implemented in subclass")

    def get_string(self):
        raise NotImplementedError("Implemented in subclass")

    def delete_all(self):
        raise NotImplementedError("Implemented in subclass")

    def incr(self, key):
        raise NotImplementedError("Implemented in subclass")

    def dumps(self):
        d = {}
        for k, v in self.items():
            d[k] = v
        return d

    def loads(self, dump):
        for k, v in dump.items():
            self[k] = v


class BigsiStorageMixin:

    ### Doesn't know the concept of a kmer
    def get_row():
        ## returns raw bytes
        pass

    def get_rows():
        ## list of raw bytes
        pass

    def set_row():
        ##
        pass

    def set_rows():
        ###
        pass

    def get_column():
        pass

    def insert_column():
        pass


class MetadataStorageMixin:

    # todo make property
    def bloomfilter():
        pass
