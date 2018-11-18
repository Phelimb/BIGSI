from bigsi.storage.base import BaseStorage
from bigsi.constants import DEFAULT_ROCKS_DB_STORAGE_CONFIG
import rocksdb
import shutil
import copy
import gc


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
        batch = rocksdb.WriteBatch()
        for k, v in zip(keys, values):
            batch.put(k, v)
        self.storage.write(batch)

    def batch_get(self, keys):
        result = self.storage.multi_get(keys)
        return [result[k] for k in keys]
