import redis
import sys
import os
from redispartition import RedisCluster
from atlasseq import hash_key

try:
    import bsddb3 as bsddb
except ImportError:
    bsddb = None


class BaseStorage(object):

    def __init__(self, config):
        """ An abstract class used as an adapter for storages. """
        raise NotImplementedError

    def __setitem__(self, key, val):
        """ Set `val` at `key`, note that the `val` must be a string. """
        raise NotImplementedError

    def __getitem__(self, key):
        """ Return `val` at `key`, note that the `val` must be a string. """
        raise NotImplementedError

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def delete_all(self):
        raise NotImplementedError

    def get_num_colours(self):
        try:
            return int(self.metadata.get('num_colours'))
        except TypeError:
            return 0


class BaseInMemoryStorage(BaseStorage):

    def __init__(self, config):
        self.name = 'dict'
        self.storage = dict()
        self.metadata = dict()
        self.secondary_storage = dict()

    def __setitem__(self, key, val):
        """ Set `val` at `key`, note that the `val` must be a string. """
        self.storage.__setitem__(key, val)

    def __getitem__(self, key):
        """ Return `val` at `key`, note that the `val` must be a string. """
        return self.storage.__getitem__(key)

    def delete_all(self):
        self.storage = dict()

    def add_sample(self, sample_name):
        existing_index = self.get_sample_colour(sample_name)
        if existing_index is not None:
            raise ValueError("%s already exists in the db" % sample_name)
        else:
            num_colours = self.get_num_colours()
            if num_colours is None:
                num_colours = 0
            else:
                num_colours = int(num_colours)
            self.metadata['s%s' % sample_name] = num_colours
            ncs = self.metadata.get('num_colours', 0)
            ncs += 1
            self.metadata['num_colours'] = ncs
            self.num_colours = self.get_num_colours()
            return num_colours

    def get_sample_colour(self, sample_name):
        c = self.metadata.get('s%s' % sample_name, None)
        if c is not None:
            return int(c)
        else:
            return c

    def colours_to_sample_dict(self):
        o = {}
        for s in self.metadata.keys():
            if s[0] == 's':
                o[int(self.metadata.get(s))] = s[1:]
        return o

    def keys(self):
        """ Returns a list of binary hashes that are used as dict keys. """
        return self.storage.keys()

    def count_keys(self):
        return len(self.storage)

    def values(self):
        return self.storage.values()

    def items(self):
        return self.storage.items()

    def getmemoryusage(self):
        d = self.storage
        size = getsizeof(d)
        size += sum(map(getsizeof, d.values())) + \
            sum(map(getsizeof, d.keys()))
        return size

    def add_to_kmers_count(self, kmers, sample):
        try:
            hll = self.metadata[sample]
        except KeyError:
            self.metadata[sample] = HyperLogLog(5)
            hll = self.metadata[sample]
        [hll.add(k) for k in kmers]

    def count_kmers(self, sample):
        hll = self.metadata[sample]
        return int(hll.cardinality())

    def insert_primary_secondary_diffs(self, primary_colour, secondary_colour, diffs):
        if not primary_colour in self.secondary_storage:
            self.secondary_storage[primary_colour] = {}

        for index in diffs:
            if not index in self.secondary_storage[primary_colour]:
                self.secondary_storage[primary_colour][index] = []
            self.secondary_storage[primary_colour][
                index].append(secondary_colour)

    def lookup_primary_secondary_diff(self, primary_colour, index):
        return self.secondary_storage[primary_colour].get(index, [])

    def diffs_between_primary_and_secondary_bloom_filter(self, primary_colour, kmers):
        raise NotImplementedError


class BaseRedisStorage(BaseStorage):

    def __init__(self, config):
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        self.name = 'redis'
        self.storage = RedisCluster([redis.StrictRedis(
            host=host, port=port, db=2) for host, port in config['conn']])
        self.metadata = RedisCluster([redis.StrictRedis(
            host=host, port=port, db=0) for host, port in config['conn']])
        self.secondary_storage = RedisCluster([redis.StrictRedis(
            host=host, port=port, db=1) for host, port in config['conn']])

    def __setitem__(self, key, val):
        name = self.get_name(key)
        self.storage.hset(name, key, val, partition_arg=1)

    def __getitem__(self, key):
        name = self.get_name(key)
        return self.storage.hget(name, key, partition_arg=1)

    def get(self, key, default=None):
        try:
            v = self[key]
            if v is None:
                return default
            else:
                return self[key]
        except KeyError:
            return default

    def get_name(self, key):
        if isinstance(key, str):
            hkey = str.encode(key)
        elif isinstance(key, int):
            hkey = (key).to_bytes(4, byteorder='big')
        name = hash_key(hkey)
        return name

    def add_sample(self, sample_name):
        existing_index = self.get_sample_colour(sample_name)
        if existing_index is not None:
            raise ValueError("%s already exists in the db" % sample_name)
        else:
            num_colours = self.get_num_colours()
            if num_colours is None:
                num_colours = 0
            else:
                num_colours = int(num_colours)
            self.metadata.set('s%s' % sample_name, num_colours)
            self.metadata.incr('num_colours')
            self.num_colours = self.get_num_colours()
            return num_colours

    def get_sample_colour(self, sample_name):
        c = self.metadata.get('s%s' % sample_name)
        if c is not None:
            return int(c)
        else:
            return c

    def keys(self, pattern="*"):
        return self.storage.keys(pattern)

    def count_keys(self):
        return self.storage.dbsize()

    def colours_to_sample_dict(self):
        o = {}
        for s in self.metadata.keys('s*'):
            o[int(self.metadata.get(s))] = s[1:].decode("utf-8")
        return o

    def delete_all(self):
        self.storage.flushall()

    def getmemoryusage(self):
        return self.storage.calculate_memory()

    def insert_kmers(self, kmers, colour):
        d = self._group_kmers_by_hashkey_and_connection(kmers)
        for conn, hk in d.items():
            _batch_insert_redis(conn, hk, colour)

    def insert_primary_secondary_diffs(self, primary_colour, secondary_colour, diffs):
        for diff in diffs:
            current_val = self.secondary_storage.hget(primary_colour, diff)
            ba = ByteArray(byte_array=current_val)
            ba.setbit(secondary_colour, 1)
            ba.to_sparse()
            self.secondary_storage.hset(primary_colour, diff, ba.bytes)

    def lookup_primary_secondary_diff(self, primary_colour, index):
        return ByteArray(self.secondary_storage.hget(primary_colour, index)).colours()

    def get_kmers(self, kmers):
        kmers = [str.encode(k) if isinstance(k, str) else k for k in kmers]
        names = [hash_key(k) for k in kmers]
        return self.storage.hget(names, kmers, partition_arg=1)

    def _group_kmers_by_hashkey_and_connection(self, kmers):
        d = dict((el, {}) for el in self.storage.connections)
        for k in kmers:
            if isinstance(k, str):
                k = str.encode(k)
            name = hash_key(k)
            conn = self.storage.get_connection(k)
            try:
                d[conn][name].append(k)
            except KeyError:
                d[conn][name] = [k]
        return d

    def add_to_kmers_count(self, kmers, sample):
        self.metadata.pfadd('kmer_count_%s' % sample, *kmers)

    def count_kmers(self, sample):
        if sample is None:
            return self.metadata.pfcount('kmer_count')
        else:
            return self.metadata.pfcount('kmer_count_%s' % sample)


class BaseBerkeleyDBStorage(BaseStorage):

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
        elif isinstance(key, int):
            key = str.encode(str(key))
        self.storage[key] = val

    def __getitem__(self, key):
        if isinstance(key, str):
            key = str.encode(key)
        elif isinstance(key, int):
            key = str.encode(str(key))
        return self.storage[key]

    def get(self, key, default=None):
        if isinstance(key, str):
            key = str.encode(key)
        elif isinstance(key, int):
            key = str.encode(str(key))
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
