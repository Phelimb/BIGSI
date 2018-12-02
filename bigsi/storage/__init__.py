from bigsi.storage.redis import RedisStorage

try:
    from bigsi.storage.berkeleydb import BerkeleyDBStorage
except ModuleNotFoundError:
    pass
try:
    from bigsi.storage.rocksdb import RocksDBStorage
except ModuleNotFoundError:
    pass


def get_storage(config):
    return {
        "rocksdb": RocksDBStorage,
        "berkeleydb": BerkeleyDBStorage,
        "redis": RedisStorage,
    }[config["storage-engine"]](config["storage-config"])
