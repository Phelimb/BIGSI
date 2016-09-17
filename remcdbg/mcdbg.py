from __future__ import print_function
import sys
from remcdbg.utils import min_lexo
from remcdbg.utils import bits
from remcdbg.utils import kmer_to_bits
from remcdbg.utils import bits_to_kmer
from remcdbg.utils import kmer_to_bytes
from remcdbg.utils import hash_key
from remcdbg.decorators import convert_kmers
from pathos.threading import ThreadPool

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

KMER_SHARDING = {}

KMER_SHARDING[0] = {0: ''}
KMER_SHARDING[1] = {0: 'A', 1: 'T', 2: 'C', 3: 'G'}
KMER_SHARDING[2] = {0: 'AA', 1: 'AT', 2: 'AC', 3: 'AG', 4: 'TA', 5: 'TT', 6: 'TC',
                    7: 'TG', 8: 'CA', 9: 'CT', 10: 'CC', 11: 'CG', 12: 'GA', 13: 'GT', 14: 'GC', 15: 'GG'}
KMER_SHARDING[3] = {0: 'AAA', 1: 'AAT', 2: 'AAC', 3: 'AAG', 4: 'ATA', 5: 'ATT', 6: 'ATC', 7: 'ATG', 8: 'ACA', 9: 'ACT', 10: 'ACC', 11: 'ACG', 12: 'AGA', 13: 'AGT', 14: 'AGC', 15: 'AGG', 16: 'TAA', 17: 'TAT', 18: 'TAC', 19: 'TAG', 20: 'TTA', 21: 'TTT', 22: 'TTC', 23: 'TTG', 24: 'TCA', 25: 'TCT', 26: 'TCC', 27: 'TCG', 28: 'TGA', 29: 'TGT', 30: 'TGC', 31:
                    'TGG', 32: 'CAA', 33: 'CAT', 34: 'CAC', 35: 'CAG', 36: 'CTA', 37: 'CTT', 38: 'CTC', 39: 'CTG', 40: 'CCA', 41: 'CCT', 42: 'CCC', 43: 'CCG', 44: 'CGA', 45: 'CGT', 46: 'CGC', 47: 'CGG', 48: 'GAA', 49: 'GAT', 50: 'GAC', 51: 'GAG', 52: 'GTA', 53: 'GTT', 54: 'GTC', 55: 'GTG', 56: 'GCA', 57: 'GCT', 58: 'GCC', 59: 'GCG', 60: 'GGA', 61: 'GGT', 62: 'GGC', 63: 'GGG'}


def execute_pipeline(p):
    p.execute()


# def logical_AND_reduce(list_of_bools):
#     return [all(l) for l in zip(*list_of_bools)]


# def logical_OR_reduce(list_of_bools):
#     return [any(l) for l in zip(*list_of_bools)]


def byte_to_bitstring(byte):
    a = str("{0:b}".format(byte))
    if len(a) < 8:
        a = "".join(['0'*(8-len(a)), a])
    return a


def _batch_insert(conn, hk, colour, count=0):
    start = time.time()
    r = conn
    colour_bytes = (colour).to_bytes(3, byteorder='big')
    with r.pipeline() as pipe:
        try:
            names = hk.keys()
            list_of_list_kmers = [v for v in hk.values()]
            pipe.watch(names)
            pipe2 = r.pipeline()
            [pipe2.hmget(name, kmers)
             for name, kmers in zip(names, list_of_list_kmers)]
            vals = pipe2.execute()
            pipe.multi()
            for name, current_vals, kmers in zip(names, vals, list_of_list_kmers):
                new_vals = {}
                for j, val in enumerate(current_vals):
                    if val is None:
                        base_bytes = b'\x00'
                        new_vals[kmers[j]] = b''.join(
                            [base_bytes, colour_bytes])
                    else:
                        new_vals[kmers[j]] = b"".join([val, colour_bytes])
                pipe.hmset(name, new_vals)
            pipe.execute()
        except redis.WatchError:
            logger.warning("Retrying %s %s " % (r, name))
            if count < 5:
                self._batch_insert(conn, hk, colour, count=count+1)
            else:
                logger.warning(
                    "Failed %s %s. Too many retries. Contining regardless." % (r, name))
    end = time.time()
    logger.info("%s seconds to process %s, %i keys" %
                (str(end-start), str(conn), len(hk)))


class McDBG(object):

    def __init__(self, conn_config, kmer_size=31, compress_kmers=True):
        # colour
        self.conn_config = conn_config
        self.hostnames = [c[0] for c in conn_config]
        self.ports = [c[1] for c in conn_config]
        self.clusters = {}
        self._create_connections()
        self.num_colours = self.get_num_colours()
        self.kmer_size = kmer_size
        self.bitpadding = 2
        self.compress_kmers = compress_kmers

    @convert_kmers
    def insert_kmer(self, kmer, colour, min_lexo=False):

        r = self.clusters['kmers'].get_connection(kmer)
        name = hash_key(kmer)
        with r.pipeline() as pipe:
            try:
                pipe.watch(name)
                if not pipe.hexists(name, kmer):
                    v = str(colour)
                else:
                    v = pipe.hget(name, kmer)
                    v = v.decode("utf-8")
                    v = ",".join([v, str(colour)])
                pipe.hset(name, kmer, v)
                pipe.execute()
            except redis.WatchError:

                self.insert_kmer(kmer, colour, min_lexo=True)

    @convert_kmers
    def get_kmer_sl(self, kmer, min_lexo=False):
        name = hash_key(kmer)
        return self.clusters['kmers'].hget(name, kmer, partition_arg=1)

    @convert_kmers
    def insert_kmers(self, kmers, colour, min_lexo=False):
        pool = ThreadPool(nodes=len(self.ports))
        d = self._group_kmers_by_hashkey_and_connection(kmers, min_lexo=True)
        # for conn, hk in d.items():
        # for name, kmers in hk.items():
        results = pool.imap(
            _batch_insert, d.keys(), [v for v in d.values()], [colour]*len(d))
        # _batch_insert(conn, hk, colour)

    @convert_kmers
    def _group_kmers_by_hashkey_and_connection(self, kmers, min_lexo=False):
        d = dict((el, {}) for el in self.clusters['kmers'].connections)
        for k in kmers:
            name = hash_key(k)
            conn = self.clusters['kmers'].get_connection(k)
            try:
                d[conn][name].append(k)
            except KeyError:
                d[conn][name] = [k]
        return d

    @convert_kmers
    def set_kmer(self, kmer, colour, min_lexo=False):
        self.clusters['kmers'].setbit(kmer, colour, 1)

    @convert_kmers
    def set_kmers(self, kmers, colour, min_lexo=False):
        self.clusters['kmers'].setbit(
            kmers, [colour]*len(kmers), [1]*len(kmers))

    @convert_kmers
    def get_kmerbit(self, kmer, colour, min_lexo=False):
        return self.clusters['kmers'].getbit(kmer, colour)

    @convert_kmers
    def sadd_kmer(self, kmer, colour, min_lexo=False):
        self.clusters['sets'].sadd(colour, kmer)

    @convert_kmers
    def srem_kmer(self, kmer, colour, min_lexo=False):
        self.clusters['sets'].srem(colour, kmer)

    @convert_kmers
    def add_kmers_to_set(self, kmers, colour, min_lexo=False):
        self.clusters['sets'].sadd(kmers, [colour]*len(kmers))

    @convert_kmers
    def search_sets(self, kmer, ignore_colour=-1, min_lexo=True):
        for i in range(self.get_num_colours()):
            if not i == ignore_colour:
                res = self.clusters['sets'].sismember(i, kmer)
                if res == 1:
                    return i
        return None

    @convert_kmers
    def add_kmers_to_list(self, kmers, colour, min_lexo=False):
        self.clusters['lists'].rpush(kmers, [colour]*len(kmers))

    @convert_kmers
    def rpush_kmer(self, kmer, colour, min_lexo=False):
        self.clusters['lists'].rpush(kmer, colour)

    def list_get_kmer(self, kmer):
        return [int(i) for i in self.clusters['lists'].lrange(kmer, -1, self.num_colours)]

    @convert_kmers
    def get_kmer_list_bitarray(self, kmer, min_lexo=False):
        l = self.list_get_kmer(kmer)
        if l:
            lb = [0]*self.num_colours
            for i in l:
                lb[i] = 1
            return tuple(lb)

    def bitops(self, kmers, op):
        byte_array = self.clusters['kmers'].bitop(
            op, str(uuid.uuid4()), *kmers)
        return self._byte_arrays_to_bits(byte_array)

    @convert_kmers
    def get_kmers(self, kmers, min_lexo=False):
        return self.clusters['kmers'].get(kmers)

    @convert_kmers
    def get_kmer(self, kmer, min_lexo=False):
        return self.clusters['kmers'].get(kmer)

    @convert_kmers
    def query_kmers(self, kmers, min_lexo=False):
        result = self.clusters['kmers'].get(kmers)
        out = []
        for kmer, _bytes in zip(kmers, result):
            if _bytes is None:
                res = self.get_kmer_list_bitarray(kmer, min_lexo=True)
                if res is None:
                    res = [0]*self.num_colours
                    i = self.search_sets(kmer, min_lexo=True)
                    if i is not None:
                        res[i] = 1
                res = tuple(res)
            else:
                res = self._byte_arrays_to_bits(_bytes)
            out.append(res)
        return out

    @convert_kmers
    def query_kmer(self, kmer, min_lexo=False):
        byte_array = self.get_kmer(kmer, min_lexo=True)
        return self._byte_arrays_to_bits(byte_array)

    @convert_kmers
    def query_kmers_100_per(self, kmers, min_lexo=False):
        res = self.bitops(kmers, "AND")
        return [bool(i) for i in res]

    @convert_kmers
    def get_non_0_kmer_colours(self, kmers, min_lexo=False):
        return [i for i, j in enumerate(
                logical_OR_reduce(self.bitops(kmers, 'OR'))) if j == 1]

    @convert_kmers
    def query_kmers_colours(self, kmers, colours=None, min_lexo=False):
        if colours is None:
            colours = self.get_non_0_kmer_colours(kmers)
        pipelines = self._create_kmer_pipeline()
        num_colours = self.get_num_colours()
        if colours:
            for kmer in kmers:
                for colour in colours:
                    pipelines[self._shard_key(kmer)].getbit(kmer, colour)
        result = self._execute_pipeline(pipelines)
        outs = [colours]
        if colours:
            for kmer in kmers:
                out = []
                for _ in colours:
                    out.append(result[self._shard_key(kmer)].pop(0))
                outs.append(tuple(out))
        return outs

    def _byte_arrays_to_bits(self, _bytes):
        logger.debug("Converting byte array to bits")
        num_colours = self.num_colours
        tmp_v = [0]*(num_colours)
        if _bytes is not None:
            tmp_v = bits(_bytes)
            logger.debug("tmp vect %s" % tmp_v)
        if len(tmp_v) < num_colours:
            tmp_v.extend([0]*(num_colours-len(tmp_v)))
        elif len(tmp_v) > num_colours:
            tmp_v = tmp_v[:num_colours]
        return tuple(tmp_v)

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
        return self.clusters['stats']

    def get_num_colours(self):
        try:
            return int(self.sample_redis.get('num_colours'))
        except TypeError:
            return 0

    def count_kmers(self):
        return self.clusters['stats'].pfcount('kmer_count')

    def count_keys(self):
        return self.clusters['kmers'].dbsize()

    def count_kmers_in_lists(self):
        return self.clusters['lists'].dbsize()

    def count_kmers_in_sets(self):
        return sum(self.clusters['sets'].scard([i for i in range(self.num_colours)]))

    def calculate_memory(self):
        # info memory returns total instance memory not memory of connectionn
        # so only need to calculate it for one DB
        memory = self.clusters['kmers'].calculate_memory()
        self.clusters['stats'].set(self.count_kmers(), memory)
        return memory

    def bitcount_all(self):
        count = Counter()
        pipelines = None
        for i, kmer in enumerate(self.kmers()):
            if i % 100000 == 0:
                if pipelines:
                    result = self._execute_pipeline(pipelines)
                    [count.update(l) for l in result.values()]
                    print(json.dumps(dict(count)))
                pipelines = self._create_kmer_pipeline(transaction=False)
                sys.stderr.write(
                    '%i of %i %f%%\n' % (i, self.count_kmers(), float(i)/self.count_kmers()))
            try:
                pipelines[self._shard_key(kmer)].bitcount(kmer)
            except KeyError:
                pass

        return dict(count)

    def compress_list(self, sparsity_threshold=0.05):
        kmers = []
        for i, kmer in enumerate(self.clusters['kmers'].scan_iter('*')):
            kmers.append(kmer)
            if i % 100000 == 0 and i > 0:
                self._batch_compress_list(
                    kmers,  sparsity_threshold=sparsity_threshold)
                kmers = []
        self._batch_compress_list(
            kmers, sparsity_threshold=sparsity_threshold)

    def uncompress_list(self, sparsity_threshold=0.05):
        kmers = []
        for i, kmer in enumerate(self.clusters['lists'].scan_iter('*')):
            kmers.append(kmer)
            if i % 100000*len(self.ports) == 0:
                self._batch_uncompress_list(
                    kmers,  sparsity_threshold=sparsity_threshold)
                kmers = []
        self._batch_uncompress_list(
            kmers, sparsity_threshold=sparsity_threshold)

    def _batch_compress_list(self, kmers, sparsity_threshold=0.05):
        if kmers:
            sparsity = [
                float(i)/self.num_colours for i in self.clusters['kmers'].bitcount(kmers)]
            compress_kmers = [
                k for i, k in enumerate(kmers) if sparsity[i] <= sparsity_threshold]
            _kmers = []
            _index = []
            for k in compress_kmers:
                bitarray = self.query_kmer(k, min_lexo=True)
                for i, j in enumerate(bitarray):
                    if j == 1:
                        _kmers.append(k)
                        _index.append(i)
            self.clusters['lists'].rpush(_kmers, _index)
            self.clusters['kmers'].delete(compress_kmers)

    def _batch_uncompress_list(self, kmers, sparsity_threshold=0.05):
        if kmers:
            sparsity = [
                float(i)/self.num_colours for i in self.clusters['lists'].llen(kmers)]
            uncompress_kmers = [
                k for i, k in enumerate(kmers) if sparsity[i] >= sparsity_threshold]
            _kmers = []
            _index = []
            for k in uncompress_kmers:
                bitarray = self.get_kmer_list_bitarray(k, min_lexo=True)
                for i, j in enumerate(bitarray):
                    if j == 1:
                        _kmers.append(k)
                        _index.append(i)
            self.clusters['kmers'].setbit(_kmers, _index, [1]*len(_kmers))
            self.clusters['lists'].delete(uncompress_kmers)

    def compress(self, **kwargs):
        self.compress_list(**kwargs)

    def uncompress(self, **kwargs):
        self.uncompress_list(**kwargs)

    def compress_hash(self):
        for cluster in ["kmers", "lists"]:
            # cluster = "kmers"
            kmers = []
            for i, kmer in enumerate(self.clusters[cluster].scan_iter('*')):
                if len(kmer) > 4:
                    kmers.append(kmer)
                    if i % 100000*len(self.ports) == 0 and i > 0:
                        self._batch_compress_hash(kmers, cluster=cluster)
                        kmers = []
            self._batch_compress_hash(kmers, cluster=cluster)

    def _batch_compress_hash(self, kmers, cluster):
        if cluster == "lists":
            vals = [",".join([str(i) for i in l]) for l in self.clusters['lists'].lrange(
                kmers, [-1]*len(kmers), [self.num_colours]*len(kmers))]
            # vals = [",".join([str(i) for i in self.list_get_kmer(k)])
            #         for k in kmers]
        else:
            vals = self.clusters[cluster].get(kmers)
        hash_keys = [hash_key(k, 3) for k in kmers]
        self.clusters[cluster].hset(hash_keys, kmers, vals, partition_arg=1)
        self.clusters[cluster].delete(kmers)

    def compress_set(self):

        kmers = []
        for i, kmer in enumerate(self.clusters['kmers'].scan_iter('*')):
            kmers.append(kmer)
            if i % 100000*len(self.ports) == 0:
                self._batch_compress(kmers)
                kmers = []
        self._batch_compress(kmers)

    def _batch_compress(self, kmers):
        if kmers:
            bitcounts = self.clusters['kmers'].bitcount(kmers)
            compress_kmers = [
                k for i, k in enumerate(kmers) if bitcounts[i] == 1]
            sorted_by_colour = self._sort_kmers_by_colour(
                compress_kmers)
            self.clusters['kmers'].delete(compress_kmers)
            self._sadd_kmers_bulk(sorted_by_colour)

    def _sort_kmers_by_colour(self, kmers):
        colours = self.clusters['kmers'].bitpos(kmers, [1]*len(kmers))
        out = {}
        for kmer, colour in zip(kmers, colours):
            try:
                out[colour].append(kmer)
            except KeyError:
                out[colour] = [kmer]
        return out

    def _sadd_kmers_bulk(self, colour_kmer_dict):
        for c, kmers in colour_kmer_dict.items():
            self.clusters['sets'].sadd([c]*len(kmers), kmers)

    def flushall(self):
        [v.flushall() for v in self.clusters.values()]

    def shutdown(self):
        [v.shutdown() for v in self.clusters.values()]

    def _kmer_to_bytes(self, kmer):
        if isinstance(kmer, str):
            return kmer_to_bytes(kmer, self.bitpadding)
        else:
            return kmer

    def _bytes_to_kmer(self, _bytes):
        bitstring = "".join([byte_to_bitstring(byte) for byte in list(_bytes)])
        return bits_to_kmer(bitstring, self.kmer_size)

    def _create_connections(self):
        # kmers stored in DB 2
        # colour arrays in DB 1
        # stats in DB 0
        self.clusters['stats'] = RedisCluster([redis.StrictRedis(
            host=host, port=port, db=0) for host, port in self.conn_config])
        self.clusters['sets'] = RedisCluster([redis.StrictRedis(
            host=host, port=port, db=1) for host, port in self.conn_config])
        self.clusters['kmers'] = RedisCluster([redis.StrictRedis(
            host=host, port=port, db=2) for host, port in self.conn_config])
        self.clusters['lists'] = RedisCluster([redis.StrictRedis(
            host=host, port=port, db=3) for host, port in self.conn_config])

    def _create_kmer_pipeline(self, transaction=True):
        # kmers stored in DB 2
        # colour arrays in DB 1
        # stats in DB 0
        pipelines = {}
        for i, port in enumerate(self.ports):
            kmer_key = KMER_SHARDING[self.sharding_level][i]
            pipelines[kmer_key] = self.connections[
                'kmers'][kmer_key].pipeline(transaction=transaction)
        return pipelines

    def _execute_pipeline(self, pipelines):
        out = {}
        for kmer_key, p in pipelines.items():
            out[kmer_key] = p.execute()
        return out
