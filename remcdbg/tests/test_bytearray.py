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
    assert r.get('tmp')[:len(a.bytes)] == a.bytes


@given(poss=st.lists(POSSIBLE_COLOUR, min_size=1), bits=st.lists(ST_BIT, min_size=1))
def test_setbit_dense_lists(poss, bits):
    r = redis.StrictRedis()
    r.flushall()
    a = ByteArray()
    for pos, bit in zip(poss, bits):
        r.setbit('tmp', pos, bit)
        a.setbit(pos, bit)
        assert r.getbit('tmp', pos) == a.getbit(pos)
    assert r.get('tmp')[:len(a.bytes)] == a.bytes


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
    dense_bytes = a.bytes
    a.to_sparse()
    if bit:
        assert a.bytes == int(pos).to_bytes(byte_order, byteorder='big')
    else:
        assert a.bytes == b''
    a.to_dense()
    assert a.bytes == dense_bytes


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
    dense_bytes = a.bytes
    dense_bitstring = a.bitstring.bin
    a.to_sparse()
    if any(bits):
        assert a.bytes == b''.join(
            [int(pos).to_bytes(byte_order, byteorder='big') for pos in positive_is])
    else:
        assert a.bytes == b''
    a.to_dense()
    assert sum(a.bytes) == sum(dense_bytes)


def test_sparse_byte_bit_encoding():
    a = ByteArray(meta=b'`')
    assert a.sparse_byte_bit_encoding == '11'
    assert a.sparse_byte_length == 4
    a._set_sparse_byte_length(3)
    assert a.sparse_byte_bit_encoding == '10'
    assert a.sparse_byte_length == 3
