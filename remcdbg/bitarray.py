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


class ByteArray(object):

    def __init__(self, meta=b'\x00', bitstring=b'\x00'):
        self.meta = BitArray(bytes=meta)
        self.bitstring = BitArray(bytes=bitstring)
        self.sparse_byte_length = 3

    def to_sparse(self):
        if not self.is_sparse():
            self.meta[0] = 1
            print('bin', self.bitstring.bin)
            for i in self.bitstring.findall('0b1'):
                print(i)
            _bytes = b''.join([int(i).to_bytes(self.sparse_byte_length, byteorder='big')
                               for i in self.bitstring.findall('0b1')])
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

    def is_sparse(self):
        # dense or sparse?
        return self.meta[0]

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
            return int(self.bitstring[pos])

    @property
    def bytes(self):
        return self.bitstring.bytes
