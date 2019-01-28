from bigsi.storage.base import BaseStorage
from bigsi.utils import batch
from bigsi.constants import DEFAULT_ROCKS_DB_STORAGE_CONFIG
import rocksdb
import shutil
import copy
import gc
import os


class RocksDB(rocksdb.DB):
    def __setitem__(self, key, val):
        self.put(key, val)

    def __getitem__(self, key):
        val = self.get(key)
        if val is None:
            raise KeyError("%s does not exist" % key)
        return val


class RocksDBStorage(BaseStorage):
    def __init__(self, storage_config=None):
        if storage_config is None:
            storage_config = DEFAULT_ROCKS_DB_STORAGE_CONFIG
        self.storage_config = copy.copy(storage_config)
        options = storage_config["options"]

        self.storage = RocksDB(
            self.storage_config["filename"], rocksdb.Options(**options)
        )
        self.write_batch_size = int(self.storage_config.get("write_batch_size", 10000))

    def __repr__(self):
        return "rocksdb storage"

    def delete_all(self):
        try:
            shutil.rmtree(self.storage_config["filename"])
        except FileNotFoundError:
            pass
        del self.storage
        RocksDBStorage.__init__(self, self.storage_config)

    def batch_set(self, keys, values):
        for batchiter in batch(zip(keys, values), self.write_batch_size):
            writebatch = rocksdb.WriteBatch()
            for k, v in batchiter:
                writebatch.put(k, v)
            self.storage.write(writebatch)

    def batch_get(self, keys):
        keys = list(keys)
        result = self.storage.multi_get(keys)
        return [result[k] for k in keys]

    def sync(self):
        gc.collect()

    def close(self):
        self.__delete_lock_file()
        del self.storage
        gc.collect()

    def __delete_lock_file(self):
        lock_file = os.path.join(self.storage_config["filename"], "LOCK")
        try:
            os.remove(lock_file)
        except (FileNotFoundError, NotADirectoryError, PermissionError):
            pass
        gc.collect()
