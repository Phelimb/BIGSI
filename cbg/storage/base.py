import redis
import sys
import os
from rediscluster import StrictRedisCluster
from cbg.utils import hash_key
from cbg.utils import chunks
from cbg.bitvector import BitArray
import shutil
import logging
import time
import crc16
import math
# from redis_protocol import encode as redis_encode
# from redis.connection import Connection
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)
from cbg.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)

try:
    import bsddb3 as bsddb
except ImportError:
    bsddb = None


from credis import Connection


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

    def incr(self, key):
        raise NotImplementedError

    def dumps(self):
        d = {}
        for k, v in self.items():
            d[k] = v
        return d

    def loads(self, dump):
        for k, v in dump.items():
            self[k] = v


class InMemoryStorage(BaseStorage):

    def __init__(self, config):
        self.name = 'dict'
        self.storage = dict()

    def __setitem__(self, key, val):
        """ Set `val` at `key`, note that the `val` must be a string. """
        self.storage.__setitem__(key, val)

    def __getitem__(self, key):
        """ Return `val` at `key`, note that the `val` must be a string. """
        return self.storage.__getitem__(key)

    def incr(self, key):
        if self.get(key) is None:
            self[key] = 0
        v = self.get(key)
        v += 1
        self[key] = v

    def delete_all(self):
        self.storage = dict()

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


class BaseRedisStorage(BaseStorage):

    def __init__(self):
        pass

    def incr(self, key):
        return self.storage.incr(key)

    def get(self, key, default=None):
        try:
            v = self[key]
            if v is None:
                return default
            else:
                return self[key]
        except KeyError:
            return default

    def keys(self, pattern="*"):
        return self.storage.keys(pattern)

    def count_keys(self):
        return self.storage.dbsize()

    def delete_all(self):
        self.storage.flushall()

    def getmemoryusage(self):
        return self.storage.calculate_memory()


class SimpleRedisStorage(BaseRedisStorage):

    def __init__(self, config, key=None):
        super().__init__()
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        self.name = 'redis'
        host, port, db = config['conn'][0]
        self.storage = redis.StrictRedis(
            host=host, port=port, db=int(db))
        self.hash_key = key

    def keys(self, pattern="*"):
        if self.hash_key:
            for key in self.storage.hkeys(self.hash_key):
                yield key.decode('utf-8')
        else:
            return self.storage.keys(pattern)

    def __setitem__(self, key, val):
        if self.hash_key:
            self.storage.hset(self.hash_key, key, val)
        else:
            self.storage.set(key, val)

    def __getitem__(self, key):
        if self.hash_key:
            v = self.storage.hget(self.hash_key, key)
            if isinstance(v, bytes):
                return v.decode('utf-8')
            else:
                return v
        else:
            return self.storage.get(key)

    def items(self):
        if self.hash_key:
            for i, j in self.storage.hgetall(self.hash_key).items():
                yield (i.decode('utf-8'), j.decode('utf-8'))
        else:
            for i in self.storage.keys():
                yield (i, self[i])


def proto(line):
    result = "*%s\r\n$%s\r\n%s\r\n" % (str(len(line)),
                                       str(len(line[0])), line[0])
    for arg in line[1:]:
        result += "$%s\r\n%s\r\n" % (str(len(arg)), arg)
    return result


class RedisBitArrayStorage(BaseRedisStorage):

    def __init__(self, config):
        super().__init__()
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        self.name = 'redis'
        self.redis_cluster = True
        startup_nodes = []
        self.max_connections = 1000
        for host, port, db in config['conn']:
            startup_nodes.append({"host": host, "port": port, "db": db})
        self.storage = StrictRedisCluster(
            startup_nodes=startup_nodes, max_connections=self.max_connections)
        self.max_bitset = 1000000
        self.cluster_info = self.get_cluster_info()
        self.cluster_slots = self.get_cluster_slots()
        self.slot_to_connection = self._init_slot_to_connection()
        self.credis = config.get('credis', True)

    def _init_connections(self):
        conns = {}
        for i, j in self.cluster_slots.items():
            host = j.get('master')[0]
            port = j.get('master')[1]
            conns[port] = Connection(host=host, port=port)
        return conns

    def _get_key_slot(self, key, method="python"):
        if method == "python":
            return crc16.crc16xmodem(str.encode(str(key))) % 16384
        else:
            return self.storage.cluster_keyslot(key)

    def _get_key_connection(self, key):
        slot = self._get_key_slot(key)
        connection = self._get_connection_of_slot(slot)
        return connection

    def _get_connection_of_slot(self, slot):
        return self.slot_to_connection[slot]

    def _init_slot_to_connection(self):
        slot_to_connection = {}
        for slot in range(16384):
            for i, j in self.cluster_slots.items():
                if i[0] <= slot <= i[1]:
                    slot_to_connection[slot] = j.get('master')
        return slot_to_connection

    def get_cluster_info(self):
        return self.storage.cluster_info()

    def get_cluster_slots(self):
        return self.storage.cluster_slots()

    def __setitem__(self, key, val):
        self.storage.set(key, val)

    def __getitem__(self, key):
        return self.storage.get(key)

    def setbit(self, index, colour, bit):
        self.storage.setbit(index, colour, bit)

    def getbit(self, index, colour):
        return self.storage.getbit(index, colour)

    def setbits(self, indexes, colour, bit):
        return self._massive_setbits(indexes, colour, bit)

    def _massive_setbits(self, indexes, colour, bit):
        if indexes:
            indexes = list(set(indexes))
            logger.debug("Setting %i bits" % len(indexes))
            logger.debug("Range %i-%i" % (min(indexes), max(indexes)))

            if self.credis:
                self.port_to_connections = self._init_connections()
                port_to_commands = {}
                for i, index in enumerate(indexes):
                    port = self._get_key_connection(index)[1]
                    try:
                        port_to_commands[port].append(
                            ("SETBIT", index, colour, bit))
                    except KeyError:
                        port_to_commands[port] = [
                            ("SETBIT", index, colour, bit)]
                    average_cmds = i / len(port_to_commands)
                    if average_cmds % 1000 == 0 and average_cmds > 0:
                        for port, commands in port_to_commands.items():
                            conn = self.port_to_connections[port]
                            if commands:
                                conn.execute_pipeline(*commands)
                                port_to_commands[port] = []
                for port, commands in port_to_commands.items():
                    conn = self.port_to_connections[port]
                    if commands:
                        conn.execute_pipeline(*commands)
            else:
                start = time.time()
                for i, _indexes in enumerate(chunks(indexes, self.max_bitset)):
                    self._setbits(_indexes, colour, bit)
                    logger.debug("%i processed in %i seconds" %
                                 ((i+1)*self.max_bitset, time.time()-start))
                end = time.time()
                logger.debug("finished setting %i bits in %i seconds" %
                             (len(indexes), end-start))

    def _setbits(self, indexes, colour, bit):
        logger.debug("Using redis-cluster pipeline")
        pipe = self.storage.pipeline()
        [pipe.setbit(i, colour, bit) for i in indexes]
        return pipe.execute()

    def getbits(self, indexes, colour):
        pipe = self.storage.pipeline()
        for i in indexes:
            pipe.getbit(i, colour)
        return pipe.execute()


class RedisHashStorage(BaseRedisStorage):

    def __init__(self, config):
        super().__init__()
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        self.name = 'redis'
        self.redis_cluster = True
        self.storage = RedisCluster([redis.StrictRedis(
            host=host, port=port, db=int(db)) for host, port, db in config['conn']])

    def __setitem__(self, key, val):
        name = self.get_name(key)
        self.storage.hset(name, key, val, partition_arg=1)

    def __getitem__(self, key):
        name = self.get_name(key)
        return self.storage.hget(name, key, partition_arg=1)

    def setbits(self, indexes, colour, bit):
        hk = self._group_kmers_by_hashkey_and_connection(indexes)
        for conn, names_hashes in hk.items():
            names = [k for k in names_hashes.keys()]
            hashes = [hs for hs in names_hashes.values()]
            _batch_insert_prob_redis(
                conn, names, hashes, colour)

    def _group_kmers_by_hashkey_and_connection(self, all_hashes):
        d = dict((el, {}) for el in self.storage.connections)
        for k in all_hashes:
            name = self.get_name(k)
            conn = self.storage.get_connection(k)
            try:
                d[conn][name].append(k)
            except KeyError:
                d[conn][name] = [k]
        return d

    def get_name(self, key):
        if isinstance(key, str):
            hkey = str.encode(key)
        elif isinstance(key, int):
            hkey = (key).to_bytes(4, byteorder='big')
        name = hash_key(hkey)
        return name


def get_vals(r, names, list_of_list_kmers):
    pipe2 = r.pipeline()
    [pipe2.hmget(name, kmers)
     for name, kmers in zip(names, list_of_list_kmers)]
    vals = pipe2.execute()
    return vals


def _batch_insert_prob_redis(conn, names, all_hashes, colour, count=0):
    r = conn
    with r.pipeline() as pipe:
        try:
            pipe.watch(names)
            vals = get_vals(r, names, all_hashes)
            pipe.multi()
            for name, values, hs in zip(names, vals, all_hashes):
                for val, h in zip(values, hs):
                    ba = BitArray()
                    if val is None:
                        val = b''
                    ba.frombytes(val)
                    ba.setbit(colour, 1)
                    pipe.hset(name, h, ba.tobytes())
            pipe.execute()
        except redis.WatchError:
            logger.warning("Retrying %s %s " % (r, name))
            if count < 5:
                self._batch_insert(conn, hk, colour, count=count+1)
            else:
                logger.warning(
                    "Failed %s %s. Too many retries. Contining regardless." % (r, name))


def _openDBEnv(cachesize):
    e = bsddb.db.DBEnv()
    if cachesize is not None:
        if cachesize >= 1:
            e.set_cachesize(cachesize, 0)
        else:
            raise error("cachesize must be >= 1")
    e.set_lk_detect(bsddb.db.DB_LOCK_DEFAULT)
    e.open('.', bsddb.db.DB_PRIVATE | bsddb.db.DB_CREATE |
           bsddb.db.DB_THREAD | bsddb.db.DB_INIT_LOCK | bsddb.db.DB_INIT_MPOOL)
    return e


# def _openDBEnvRead(cachesize):
#     e = bsddb.db.DBEnv()
#     if cachesize is not None:
#         if cachesize >= 1:
#             e.set_cachesize(cachesize, 0)
#         else:
#             raise error("cachesize must be >= 1")
#     # e.set_lk_detect(bsddb.db.DB_LOCK_DEFAULT)
#     e.open('.', bsddb.db.DB_PRIVATE | bsddb.db.DB_CREATE |
#            bsddb.db.DB_THREAD | bsddb.db.DB_INIT_MPOOL)
#     return e


def hashopen(file, flag='c', mode=0o666, pgsize=None, ffactor=None, nelem=None,
             cachesize=None, lorder=None, hflags=0):

    flags = bsddb._checkflag(flag, file)
    e = _openDBEnv(cachesize)
    d = bsddb.db.DB(e)
    d.set_flags(hflags)
    if pgsize is not None:
        d.set_pagesize(pgsize)
    if lorder is not None:
        d.set_lorder(lorder)
    if ffactor is not None:
        d.set_h_ffactor(ffactor)
    if nelem is not None:
        d.set_h_nelem(nelem)
    d.open(file, bsddb.db.DB_HASH, flags, mode)
    return bsddb._DBWithCursor(d)


class BerkeleyDBCollectionStorage(BaseStorage):

    def __init__(self, row_orded_filenames, rows_per_file=10000):
        self.dbs = {}
        self._create_berkeley_dbs(row_orded_filenames)
        self.rows_per_file = rows_per_file

    def _create_berkeley_dbs(self, row_orded_filenames):
        self.dbs = {}
        for i, f in enumerate(row_orded_filenames):
            self.dbs[i] = f

    def __getitem__(self, key):
        assert isinstance(key, int)
        f = self.dbs[int(math.floor(key/self.rows_per_file))]
        return BerkeleyDBStorage({"filename": f})[key]


class BerkeleyDBStorage(BaseStorage):

    def __init__(self, config):
        if 'filename' not in config:
            raise ValueError(
                "You must supply a 'filename' in your config%s" % config)
        self.db_file = config['filename']
        self.mode = config.get('mode', 'c')
        self.cachesize = config.get('cachesize', 4)
        try:
            self.storage = hashopen(
                self.db_file, flag=self.mode, cachesize=self.cachesize)
        except AttributeError:
            raise ValueError(
                "Please install bsddb3 to use berkeley DB storage")
        self.decode = config.get('decode', None)

    def incr(self, key):
        if self.get(key) is None:
            self[key] = 0
        v = int(self.get(key))
        v += 1
        self[key] = v

    def __exit__(self, type, value, traceback):
        self.storage.sync()

    def keys(self):
        return self.storage.keys()

    def items(self):
        for i in self.storage.keys():
            yield (i.decode('utf-8'), self[i])

    def count_keys(self):
        return len(self.keys())

    def __setitem__(self, key, val):
        if isinstance(key, str):
            key = str.encode(key)
        elif isinstance(key, int):
            key = str.encode(str(key))
            # key = (key).to_bytes(4, byteorder='big')
        if isinstance(val, str):
            val = str.encode(val)
        elif isinstance(val, int):
            val = str.encode(str(val))
        self.storage[key] = val
        if self.decode:
            self.storage.sync()

    def __getitem__(self, key):
        if isinstance(key, str):
            key = str.encode(key)
        elif isinstance(key, int):
            key = str.encode(str(key))
            # key = (key).to_bytes(4, byteorder='big')
        v = self.storage[key]
        if self.decode:
            return v.decode(self.decode)
        else:
            return v

    def __delitem__(self, key):
        if isinstance(key, str):
            key = str.encode(key)
        elif isinstance(key, int):
            key = str.encode(str(key))
        del self.storage[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def delete_all(self):
        self.storage.close()
        os.remove(self.db_file)
        self.storage = hashopen(self.db_file)

    def getmemoryusage(self):
        return 0


class LevelDBStorage(BaseStorage):

    def __init__(self, config):
        if 'filename' not in config:
            raise ValueError(
                "You must supply a 'filename' in your config%s" % config)
        self.db_file = config['filename']
        try:
            self.storage = leveldb.DB(
                self.db_file.encode(), create_if_missing=True)
        except AttributeError:
            raise ValueError(
                "Please install leveldb to use level DB storage")

    def incr(self, key):
        if self.get(key) is None:
            self[key] = 0
        v = int(self.get(key))
        v += 1
        self[key] = v

    def keys(self):
        return self.storage.keys()

    def items(self):
        for i in self.storage.keys():
            yield (i.decode('utf-8'), self[i].decode('utf-8'))

    def count_keys(self):
        return len(self.keys())

    def __setitem__(self, key, val):
        if isinstance(key, str):
            key = str.encode(key)
        elif isinstance(key, int):
            key = str.encode(str(key))
        if isinstance(val, str):
            val = str.encode(val)
        elif isinstance(val, int):
            val = str.encode(str(val))
        self.storage.put(key, val)

    def __getitem__(self, key):
        if isinstance(key, str):
            key = str.encode(key)
        elif isinstance(key, int):
            key = str.encode(str(key))
        return self.storage.get(key)

    def get(self, key, default=None):
        if isinstance(key, str):
            key = str.encode(key)
        elif isinstance(key, int):
            key = str.encode(str(key))
        try:
            v = self[key]
            if v is None:
                return default
            else:
                return v
        except KeyError:
            return default

    def delete_all(self):
        self.storage.close()
        shutil.rmtree(self.db_file)
        self.storage = leveldb.DB(
            self.db_file.encode(), create_if_missing=True)

    def getmemoryusage(self):
        return 0
