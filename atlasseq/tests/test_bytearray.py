from atlasseq.tests.base import REDIS_PORT
from atlasseq.tests.base import REDIS_HOST

from atlasseq.bytearray import ByteArray
from hypothesis import given
import hypothesis.strategies as st
import redis
import os


def test_sparse_dense_metadata():
    a = ByteArray()
    assert a.is_sparse() == False
    a.to_sparse()
    assert a.is_sparse() == True
    a.to_dense()
    a.is_sparse() == False
ST_BIT = st.integers(min_value=0, max_value=1)


POSSIBLE_COLOUR = st.integers(min_value=0, max_value=65536)


@given(pos=POSSIBLE_COLOUR, bit=ST_BIT)
def test_setbit_dense(pos, bit):
    r = redis.StrictRedis(REDIS_HOST, REDIS_PORT)
    r.flushall()
    r.setbit('tmp', pos, bit)
    a = ByteArray()
    a.setbit(pos, bit)
    if bit:
        assert a.indexes() == [pos]
    assert r.getbit('tmp', pos) == a.getbit(pos)
    assert r.get('tmp')[:len(a.bitstring.tobytes())] == a.bitstring.tobytes()


@given(poss=st.lists(POSSIBLE_COLOUR, min_size=1), bits=st.lists(ST_BIT, min_size=1))
def test_setbit_dense_lists(poss, bits):
    r = redis.StrictRedis(REDIS_HOST, REDIS_PORT)
    r.flushall()
    a = ByteArray()
    for pos, bit in zip(poss, bits):
        r.setbit('tmp', pos, bit)
        a.setbit(pos, bit)
        assert r.getbit('tmp', pos) == a.getbit(pos)
#    assert sorted(a.indexes()) == sorted(list(
#        set([pos for pos, bit in zip(poss, bits) if bit])))

    assert r.get('tmp')[:len(a.bitstring.tobytes())] == a.bitstring.tobytes()


@given(pos=POSSIBLE_COLOUR, bit=ST_BIT)
def test_convert_to_dense(pos, bit):
    if pos <= 255:
        byte_order = 1
    elif 255 <= pos <= 65535:
        byte_order = 2
    else:
        byte_order = 3
    a = ByteArray()
    a.setbit(pos, bit)
    dense_bytes = a.bitstring.tobytes()

    a.to_sparse()

    if bit:
        assert a.bitstring.tobytes() == int(
            pos).to_bytes(byte_order, byteorder='big')
    else:
        assert a.bitstring.tobytes() == b''
    a.to_dense()
    assert a.bitstring.tobytes() == dense_bytes


@given(poss=st.lists(POSSIBLE_COLOUR, min_size=1), bits=st.lists(ST_BIT, min_size=1))
def test_convert_to_dense_list(poss, bits):
    a = ByteArray()
    poss = sorted(poss)
    positive_is = set()
    byte_order = 1
    for pos, bit in zip(poss, bits):
        if bit:
            positive_is.add(pos)
            if 255 < pos <= 65535 and byte_order < 3:
                byte_order = 2
            elif pos >= 65535 and byte_order < 3:
                byte_order = 3
        else:
            positive_is.discard(pos)
    positive_is = sorted(list(positive_is))

    for pos, bit in zip(poss, bits):
        a.setbit(pos, bit)
    dense_bytes = a.bitstring.tobytes()
    a.to_sparse()
    if any(bits):
        assert a.bitstring.tobytes() == b''.join(
            [int(pos).to_bytes(byte_order, byteorder='big') for pos in positive_is])
    else:
        assert a.bitstring.tobytes() == b''
    a.to_dense()
    assert sum(a.bitstring.tobytes()) == sum(dense_bytes)


def test_sparse_byte_bit_encoding():
    a = ByteArray(meta=b'`')
    assert a.sparse_byte_bit_encoding == '11'
    assert a.sparse_byte_length == 4
    a._set_sparse_byte_length(3)
    assert a.sparse_byte_bit_encoding == '10'
    assert a.sparse_byte_length == 3


@given(poss=st.lists(POSSIBLE_COLOUR, min_size=1), bits=st.lists(ST_BIT, min_size=1))
def test_choose_optimal_encoding(poss, bits):
    a = ByteArray()
    for pos, bit in zip(poss, bits):
        a.setbit(pos, bit)
    dense_bytes = a.bitstring.tobytes()
    a.to_sparse()
    sparse_bytes = a.bitstring.tobytes()
    a.choose_optimal_encoding()
    if a.is_sparse():
        assert len(a.bitstring.tobytes()) <= len(dense_bytes)
    elif a.is_dense():
        assert len(a.bitstring.tobytes()) <= len(sparse_bytes)


def test_choose_optimal_encoding2():
    a = ByteArray(
        byte_array=b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10')
    a.choose_optimal_encoding()
    assert a.is_sparse()
    assert a.bin == '101000000000010011110011'
    assert str(a) == '101000000000010011110011'
    assert a.__repr__() == '101000000000010011110011'

# Test adding to sparse array


@given(poss=st.lists(POSSIBLE_COLOUR, min_size=1), bits=st.lists(ST_BIT, min_size=1))
def test_setbit_sparse_lists(poss, bits):
    r = redis.StrictRedis(REDIS_HOST, REDIS_PORT)
    r.flushall()
    a = ByteArray()
    a.to_sparse()
    for pos, bit in zip(poss, bits):
        r.setbit('tmp', pos, bit)
        a.setbit(pos, bit)
        assert r.getbit('tmp', pos) == a.getbit(pos)
    a.to_dense()
    assert r.get('tmp')[:len(a.bitstring.tobytes())] == a.bitstring.tobytes()


@given(colours1=st.lists(POSSIBLE_COLOUR, min_size=1), colours2=st.lists(POSSIBLE_COLOUR, min_size=1))
def test_intersect(colours1, colours2):
    a1 = ByteArray()
    a2 = ByteArray()
    for c in colours1:
        a1.setbit(c, 1)
    for c in colours2:
        a2.setbit(c, 1)
    assert sorted(a1.intersect(a2).colours()) == sorted(
        list(set(colours1) & set(colours2)))
