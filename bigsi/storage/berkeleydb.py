from bigsi.storage.base import BaseStorage
from bigsi.constants import DEFAULT_BERKELEY_DB_STORAGE_CONFIG
import bsddb3
import os


class BerkeleyDBStorage(BaseStorage):
    def __init__(self, storage_config=None):
        if storage_config is None:
            storage_config = DEFAULT_BERKELEY_DB_STORAGE_CONFIG
        self.storage_config = storage_config
        self.storage = bsddb3.hashopen(
            storage_config["filename"],
            flag="c",
            cachesize=storage_config.get("hashsize", 20480),
        )

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
