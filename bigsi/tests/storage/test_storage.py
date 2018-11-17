"""
Base storages can store, integers, strings, and byte strings
"""
from bigsi.storage import RedisStorage
from bigsi.storage import BerkeleyDBStorage
from bigsi.storage import RocksDBStorage
from bitarray import bitarray
import pytest

STORAGES = [RedisStorage(), BerkeleyDBStorage(), RocksDBStorage()]


def test_get_set():
    for storage in STORAGES:
        storage["test"] = b"123"
        assert storage["test"] == b"123"


def test_get_set_integer():
    for storage in STORAGES:
        storage.set_integer("test", 112)
        assert storage.get_integer("test") == 112


def test_get_set_string():
    for storage in STORAGES:
        storage.set_string("test", "abc")
        assert storage.get_string("test") == "abc"


def test_get_set_bitarray():
    ba = bitarray("110101111010")
    for storage in STORAGES:
        storage.set_bitarray("test", ba)
        assert storage.get_bitarray("test") == ba
        assert storage.get_bit("test", 1) == True
        assert storage.get_bit("test", 2) == False
        storage.set_bit("test", 0, 0)
        assert storage.get_bitarray("test") == bitarray("010101111010")
        assert storage.get_bit("test", 0) == False


# def test_delete():
#     for storage in STORAGES:
#         storage.set_string("test", "1231")
#         assert storage.get_string("test") == "1231"
#         storage.delete_all()
#         with pytest.raises(BaseException):
#             storage.get_string("test") == None
