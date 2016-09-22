from __future__ import print_function
import sys
from remcdbg.utils import min_lexo
from remcdbg.utils import bits
from remcdbg.utils import kmer_to_bits
from remcdbg.utils import bits_to_kmer
from remcdbg.utils import kmer_to_bytes
from remcdbg.utils import hash_key
from remcdbg.storage import choose_storage
from remcdbg.bytearray import ByteArray
from remcdbg.decorators import convert_kmers
sys.path.append("../cortex-py")
from mccortex.cortex import encode_kmer
from mccortex.cortex import decode_kmer
# sys.path.append("../redis-py-partition")
from redispartition import RedisCluster
import redis
import math
import uuid
import time
from collections import Counter
import json
import logging
logging.basicConfig()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# def byte_to_bitstring(byte):
#     a = str("{0:b}".format(byte))
#     if len(a) < 8:
#         a = "".join(['0'*(8-len(a)), a])
#     return a


def _batch_compress(conn, hk, count=0):
    r = conn
    with r.pipeline() as pipe:
        try:
            names = [k for k in hk.keys()]
            list_of_list_kmers = [v for v in hk.values()]
            pipe.watch(names)
            vals = get_vals(r, names, list_of_list_kmers)
            pipe.multi()
            for name, current_vals, kmers in zip(names, vals, list_of_list_kmers):
                new_vals = {}
                for j, val in enumerate(current_vals):
                    ba = ByteArray(byte_array=val)
                    ba.choose_optimal_encoding()
                    new_vals[kmers[j]] = ba.bytes
                pipe.hmset(name, new_vals)
            pipe.execute()
        except redis.WatchError:
            logger.warning("Retrying %s %s " % (r, name))
            if count < 5:
                self._batch_insert(conn, hk, count=count+1)
            else:
                logger.warning(
                    "Failed %s %s. Too many retries. Contining regardless." % (r, name))


class McDBG(object):

    def __init__(self, conn_config, kmer_size=31, compress_kmers=True, storage={'dict': None}):
        # colour
        self.conn_config = conn_config
        self.hostnames = [c[0] for c in conn_config]
        self.ports = [c[1] for c in conn_config]
        self.clusters = {}
        self._create_connections()
        self.num_colours = self.get_num_colours()
        self.kmer_size = kmer_size
        self.bitpadding = 2  # TODO. This should be dependant on kmer size
        self.compress_kmers = compress_kmers
        self.storage = choose_storage(storage)

    @convert_kmers
    def insert_kmer(self, kmer, colour, min_lexo=False):
        self.storage.insert_kmer(kmer, colour)

    @convert_kmers
    def insert_kmers(self, kmers, colour, min_lexo=False):
        self.storage.insert_kmers(kmers, colour)

    @convert_kmers
    def get_kmer_raw(self, kmer, min_lexo=False):
        return self.storage.get_kmer(kmer)

    @convert_kmers
    def get_kmers_raw(self, kmers, min_lexo=False):
        return self.storage.get_kmers(kmers)

    @convert_kmers
    def get_kmer(self, kmer, min_lexo=False):
        raw = self.get_kmer_raw(kmer, min_lexo=True)
        return ByteArray(byte_array=raw)

    @convert_kmers
    def get_kmers(self, kmers, min_lexo=False):
        raws = self.get_kmers_raw(kmers, min_lexo=True)
        return [ByteArray(raw) for raw in raws]

    @convert_kmers
    def get_kmer_colours(self, kmer, min_lexo=False):
        ba = self.get_kmer(kmer, min_lexo=True)
        return {kmer: ba.colours()}

    @convert_kmers
    def get_kmers_colours(self, kmers, min_lexo=False):
        bas = self.get_kmers(kmers, min_lexo=True)
        o = {}
        for kmer, bas in zip(kmers, bas):
            o[kmer] = bas.colours()
        return o

    @convert_kmers
    def query_kmer(self, kmer, min_lexo=False):
        out = {}
        colours_to_sample_dict = self.colours_to_sample_dict()
        for colour in self.get_kmer_colours(kmer, min_lexo=True):
            sample = colours_to_sample_dict.get(colour, 'missing')
            out[sample] = 1
        return out

    @convert_kmers
    def query_kmers(self, kmers, min_lexo=False, threshold=1):
        colours_to_sample_dict = self.colours_to_sample_dict()
        tmp = Counter()
        for kmer, colours in self.get_kmers_colours(kmers, min_lexo=True).items():
            tmp.update(colours)
        out = {}
        for k, f in tmp.items():
            res = f/len(kmers)
            if res >= threshold:
                out[colours_to_sample_dict.get(k, k)] = res
        return out

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
            self.sample_redis.set('s%s' % sample_name, num_colours)
            self.sample_redis.incr('num_colours')
            self.num_colours = self.get_num_colours()
            return num_colours

    def get_sample_colour(self, sample_name):
        c = self.sample_redis.get('s%s' % sample_name)
        if c is not None:
            return int(c)
        else:
            return c

    def colours_to_sample_dict(self):
        o = {}
        for s in self.sample_redis.keys('s*'):
            o[int(self.sample_redis.get(s))] = s[1:].decode("utf-8")
        return o

    @property
    def sample_redis(self):
        return self.clusters['stats'].connections[0]

    def get_num_colours(self):
        try:
            return int(self.sample_redis.get('num_colours'))
        except TypeError:
            return 0

    def count_kmers(self):
        return self.clusters['stats'].pfcount('kmer_count')

    def count_keys(self):
        return self.storage.count_keys()

    def calculate_memory(self):
        return self.storage.getmemoryusage()

    def delete_all(self):
        self.storage.delete_all()
        [v.flushall() for v in self.clusters.values()]

    def shutdown(self):
        [v.shutdown() for v in self.clusters.values()]

    def _kmer_to_bytes(self, kmer):
        if isinstance(kmer, str):
            return encode_kmer(kmer)
        else:
            return kmer

    def _bytes_to_kmer(self, _bytes):
        #        bitstring = ByteArray(byte_array=_bytes).bin[::-1]
        # bits_to_kmer(bitstring, self.kmer_size)
        return decode_kmer(_bytes, kmer_size=self.kmer_size)

    def _create_connections(self):
        # kmers stored in DB 2
        # stats in DB 0
        self.clusters['stats'] = RedisCluster([redis.StrictRedis(
            host=host, port=port, db=0) for host, port in self.conn_config])
