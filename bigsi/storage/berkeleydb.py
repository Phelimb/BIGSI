from bigsi.storage.base import BaseStorage
from bigsi.constants import DEFAULT_BERKELEY_DB_STORAGE_CONFIG
from bsddb3 import db
import os

class BerkeleyDBStorage(BaseStorage):
    def __init__(self, storage_config=None):
        if storage_config is None:
            storage_config = DEFAULT_BERKELEY_DB_STORAGE_CONFIG
        self.storage_config = storage_config

        self.storage = db.DB()

        GB = 1024 * 1024 * 1024;
        self.storage.set_cachesize(
            int(storage_config.get("hashsize", 204800) / GB),
            int(storage_config.get("hashsize", 204800) % GB))

        self.storage.open(storage_config["filename"], None, db.DB_HASH, db.DB_CREATE)

    def __repr__(self):
        return "berkeleydb Storage"

    def delete_all(self):
        self.storage.close()
        try:
            os.remove(self.storage_config["filename"])
        except FileNotFoundError:
            pass
        BerkeleyDBStorage.__init__(self, storage_config=self.storage_config)

    def sync(self):
        self.storage.sync()
