from __future__ import print_function
from bitstring import BitArray
import sys
import math


def setbit(bitstring, pos, i):
    try:
        bitstring.set(value=i, pos=pos)
    except IndexError:
        if i:
            bitstring.append(b"".join([
                b'\x00']*math.ceil(float(1+pos-len(bitstring))/8)))
            setbit(bitstring, pos, i)
    return bitstring

BITS_TO_BYTE_LENGTH = {'00': 1, '01': 2, '10': 3, '11': 4}
BYTE_LENGTH_TO_BITS = {v: k for k, v in BITS_TO_BYTE_LENGTH.items()}


def choose_int_encoding(ints):
    if all([pos <= 255 for pos in ints]):
        byteorder = 1
    elif all([pos <= 65535 for pos in ints]):
        byteorder = 2
    else:
        byteorder = 3
    return byteorder


class ByteArray(object):

    def __init__(self, meta=b'\x00', bitstring=b'\x00'):
        self.meta = BitArray(bytes=meta)
        self.bitstring = BitArray(bytes=bitstring)

    def is_sparse(self):
        # dense or sparse?
        return self.meta[0]

    def is_dense(self):
        return not self.is_sparse()

    @property
    def sparse_byte_bit_encoding(self):
        return self.meta.bin[1:3]

    @property
    def sparse_byte_length(self):
        return BITS_TO_BYTE_LENGTH[self.sparse_byte_bit_encoding]

    def _set_sparse_byte_length(self, l):
        tmp = list(self.meta.bin)
        tmp[1:3] = list(BYTE_LENGTH_TO_BITS[l])
        self.meta = BitArray(bin="".join(tmp))

    def to_sparse(self):
        if not self.is_sparse():
            self.meta[0] = 1
            indexes = [i for i in self.bitstring.findall('0b1')]
            bo = choose_int_encoding(indexes)
            self._set_sparse_byte_length(bo)
            _bytes = b''.join([int(i).to_bytes(self.sparse_byte_length, byteorder='big')
                               for i in indexes])
            self.bitstring = BitArray(bytes=_bytes)

    def to_dense(self):
        if self.is_sparse():
            new_bitstring = BitArray(b'\x00')
            for i in self._bit_1_indexes():
                setbit(new_bitstring, i, 1)
            self.bitstring = new_bitstring
            self.meta[0] = 0

    def _bit_1_indexes(self):
        s = self.sparse_byte_length
        _bytes = self.bitstring.bytes
        assert self.is_sparse()
        return [int.from_bytes(_bytes[i*s:(i+1)*s], byteorder='big') for i in range(0, int(len(_bytes)/s))]

    def setbit(self, pos, i):
        if self.is_sparse():
            raise NotImplementedError()
        else:
            self._setbit_dense(pos, i)

    def _setbit_dense(self, pos, i):
        self.bitstring = setbit(self.bitstring, pos, i)

    def getbit(self, pos):
        if self.is_sparse():
            raise NotImplementedError()
        else:
            try:
                return int(self.bitstring[pos])
            except IndexError:
                return 0

    @property
    def bytes(self):
        return self.bitstring.bytes
