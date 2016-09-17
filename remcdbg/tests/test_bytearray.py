from remcdbg.bitarray import ByteArray
from hypothesis import given
import hypothesis.strategies as st
import redis


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
    r = redis.StrictRedis()
    r.flushall()
    r.setbit('tmp', pos, bit)
    a = ByteArray()
    a.setbit(pos, bit)
    assert r.getbit('tmp', pos) == a.getbit(pos)
    assert r.get('tmp')[:len(a.bitstring.bytes)] == a.bitstring.bytes


@given(poss=st.lists(POSSIBLE_COLOUR, min_size=1), bits=st.lists(ST_BIT, min_size=1))
def test_setbit_dense_lists(poss, bits):
    r = redis.StrictRedis()
    r.flushall()
    a = ByteArray()
    for pos, bit in zip(poss, bits):
        r.setbit('tmp', pos, bit)
        a.setbit(pos, bit)
        assert r.getbit('tmp', pos) == a.getbit(pos)
    assert r.get('tmp')[:len(a.bitstring.bytes)] == a.bitstring.bytes


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
    dense_bytes = a.bitstring.bytes
    a.to_sparse()
    if bit:
        assert a.bitstring.bytes == int(
            pos).to_bytes(byte_order, byteorder='big')
    else:
        assert a.bitstring.bytes == b''
    a.to_dense()
    assert a.bitstring.bytes == dense_bytes


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
    dense_bytes = a.bitstring.bytes
    dense_bitstring = a.bitstring.bin
    a.to_sparse()
    if any(bits):
        assert a.bitstring.bytes == b''.join(
            [int(pos).to_bytes(byte_order, byteorder='big') for pos in positive_is])
    else:
        assert a.bitstring.bytes == b''
    a.to_dense()
    assert sum(a.bitstring.bytes) == sum(dense_bytes)


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
    dense_bytes = a.bitstring.bytes
    a.to_sparse()
    sparse_bytes = a.bitstring.bytes
    a.choose_optimal_encoding()
    if a.is_sparse():
        assert len(a.bitstring.bytes) <= len(dense_bytes)
    elif a.is_dense():
        assert len(a.bitstring.bytes) <= len(sparse_bytes)

# Test adding to sparse array


@given(poss=st.lists(POSSIBLE_COLOUR, min_size=1), bits=st.lists(ST_BIT, min_size=1))
def test_setbit_sparse_lists(poss, bits):
    r = redis.StrictRedis()
    r.flushall()
    a = ByteArray()
    a.to_sparse()
    for pos, bit in zip(poss, bits):
        r.setbit('tmp', pos, bit)
        a.setbit(pos, bit)
        assert r.getbit('tmp', pos) == a.getbit(pos)
    a.to_dense()
    assert r.get('tmp')[:len(a.bitstring.bytes)] == a.bitstring.bytes
