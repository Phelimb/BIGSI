from bigsi.storage.redis import RedisStorage

STORAGE_DICT = {"redis": RedisStorage}
try:
    from bigsi.storage.berkeleydb import BerkeleyDBStorage
except ModuleNotFoundError:
    pass
else:
    STORAGE_DICT["berkeleydb"] = BerkeleyDBStorage
try:
    from bigsi.storage.rocksdb import RocksDBStorage
except ModuleNotFoundError:
    pass
else:
    STORAGE_DICT["rocksdb"] = RocksDBStorage


def get_storage(config):
    return STORAGE_DICT[config["storage-engine"]](config["storage-config"])
