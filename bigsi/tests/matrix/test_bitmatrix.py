"""
BIGSI storages can bitarray rows and metadata
"""
from bigsi.storage import RedisStorage
from bigsi.storage import BerkeleyDBStorage
from bigsi.storage import RocksDBStorage
from bigsi.matrix import BitMatrix
from bitarray import bitarray
import pytest


def get_storages():
    return [RedisStorage(), BerkeleyDBStorage(), RocksDBStorage()]


def test_get_set():
    rows = [
        bitarray("001"),
        bitarray("001"),
        bitarray("111"),
        bitarray("001"),
        bitarray("111"),
    ] * 5
    for storage in get_storages():
        storage.delete_all()
        bm = BitMatrix.create(storage, rows, len(rows))
        bm.set_rows(range(25), rows)
        assert list(bm.get_rows(range(3))) == rows[:3]
        assert bm.get_column(0) == bitarray("00101" * 5)
        assert bm.get_column(2) == bitarray("1" * 25)
        assert list(bm.get_columns([0, 2])) == [
            bitarray("00101" * 5),
            bitarray("1" * 25),
        ]


def test_get_insert_column():
    rows = [
        bitarray("001"),
        bitarray("001"),
        bitarray("111"),
        bitarray("001"),
        bitarray("111"),
    ] * 5
    for storage in get_storages():
        storage.delete_all()
        bm = BitMatrix.create(storage, rows, len(rows))
        assert bm.get_column(0) == bitarray("00101" * 5)
        bm.insert_column(bitarray("1" * 25), 0)
        assert bm.get_column(0) == bitarray("1" * 25)

        assert bm.get_row(1) == bitarray("101")
        bm.insert_column(bitarray("1" * 25), 3)
        assert bm.get_column(3) == bitarray("1" * 25)
        assert bm.get_row(1) == bitarray("1011")
