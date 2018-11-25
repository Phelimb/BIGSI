DEFAULT_ROCKS_DB_STORAGE_CONFIG = {
    "filename": "test-rocksdb",
    "options": {"max_open_files": 5000, "create_if_missing": True},
}

DEFAULT_BERKELEY_DB_STORAGE_CONFIG = {"filename": "test-berkeleydb"}

DEFAULT_REDIS_STORAGE_CONFIG = {"host": "localhost", "port": 6379}
DEFAULT_PARAMETERS = {"k": 31, "m": 25 * 10 ** 6, "h": 3}
DEFAULT_ROCKS_DB_CONFIG = {
    "type": "rocksdb",
    "storage": DEFAULT_ROCKS_DB_STORAGE_CONFIG,
    **DEFAULT_PARAMETERS,
}

DEFAULT_REDIS_CONFIG = {
    "type": "redis",
    "storage": DEFAULT_REDIS_STORAGE_CONFIG,
    **DEFAULT_PARAMETERS,
}

DEFAULT_BERKELEY_DB_CONFIG = {
    "type": "berkeleydb",
    "storage": DEFAULT_BERKELEY_DB_STORAGE_CONFIG,
    **DEFAULT_PARAMETERS,
}

DEFAULT_CONFIG = DEFAULT_ROCKS_DB_CONFIG
DEFAULT_NPROC = 4
