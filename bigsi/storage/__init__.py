from bigsi.storage.berkeleydb import BerkeleyDBStorage
from bigsi.storage.redis import RedisStorage
from bigsi.storage.rocksdb import RocksDBStorage


def get_storage(config):
    return {
        "rocksdb": RocksDBStorage,
        "berkeleydb": BerkeleyDBStorage,
        "redis": RedisStorage,
    }[config["storage-engine"]](config["storage-config"])
