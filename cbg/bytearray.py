from __future__ import print_function
# from bitstring import BitArray
from cbg.bitvector import BitArray
import sys
import math


def setbit(bitstring, pos, i):
    try:
        bitstring[pos] = bool(i)  # .set(value=i, pos=pos)
    except IndexError:
        if i:
            for _ in range(1+pos-len(bitstring)):
                bitstring.append(False)
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

    def __init__(self, byte_array=None, meta=b'\x00', bitstring=b'\x00'):
        self.meta = BitArray()
        self.bitstring = BitArray()
        if byte_array is None:
            self.meta.frombytes(meta)
            self.bitstring.frombytes(bitstring)
        else:
            self.meta.frombytes(byte_array[0:1])
            self.bitstring.frombytes(byte_array[1:])

    def intersect(self, ba):
        colours = set(self.colours()) & set(ba.colours())
        new = ByteArray()
        for c in colours:
            new.setbit(c, 1)
        return new

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self.bin

    def is_sparse(self):
        # dense or sparse?
        return self.meta[0]

    def is_dense(self):
        return not self.is_sparse()

    def colours(self):
        if self.is_sparse():
            return self._bit_1_indexes()
        else:
            return self.indexes()

    @property
    def sparse_byte_bit_encoding(self):
        return "".join([str(int(i)) for i in self.meta[1:3]])

    @property
    def sparse_byte_length(self):
        return BITS_TO_BYTE_LENGTH[self.sparse_byte_bit_encoding]

    def _set_sparse_byte_length(self, l):
        self.meta[1] = bool(int(BYTE_LENGTH_TO_BITS[l][0]))
        self.meta[2] = bool(int(BYTE_LENGTH_TO_BITS[l][1]))

    def to_sparse(self):
        if self.is_dense():
            indexes = self.indexes()
            self.meta[0] = True

            bo = choose_int_encoding(indexes)
            self._set_sparse_byte_length(bo)
            _bytes = b''.join([int(i).to_bytes(self.sparse_byte_length, byteorder='big')
                               for i in indexes])
            self.bitstring = BitArray()
            self.bitstring.frombytes(_bytes)

    def indexes(self):
        indexes = []
        i = 0
        if self.is_dense():
            while True:
                try:
                    i = self.bitstring.index(True, i)
                    indexes.append(i)
                    i += 1
                except ValueError:
                    break
        return indexes

    def to_dense(self):
        if self.is_sparse():
            new_bitstring = BitArray()
            new_bitstring.frombytes(b'\x00')
            for i in self._bit_1_indexes():
                setbit(new_bitstring, i, 1)
            self.bitstring = new_bitstring
            self.meta[0] = False

    def _bit_1_indexes(self):
        s = self.sparse_byte_length
        _bytes = self.bitstring.tobytes()
        assert self.is_sparse()
        return [int.from_bytes(_bytes[i*s:(i+1)*s], byteorder='big') for i in range(0, int(len(_bytes)/s))]

    def setbit(self, pos, i):
        if self.is_sparse():
            self._setbit_sparse(pos, i)
        else:
            self._setbit_dense(pos, i)

    def _setbit_dense(self, pos, i):
        self.bitstring = setbit(self.bitstring, pos, i)

    def _setbit_sparse(self, pos, i):

        if i == 0:
            self.to_dense()
            self._setbit_dense(pos, i)
            self.to_sparse()
        else:
            if not pos in self.colours():

                if choose_int_encoding([pos]) > self.sparse_byte_length:
                    # lazy option
                    self.to_dense()
                    self._setbit_dense(pos, i)
                    self.to_sparse()

                else:
                    _append_bytes = int(pos).to_bytes(
                        self.sparse_byte_length, byteorder='big')
                    b = b''.join([self.bitstring.tobytes(), _append_bytes])
                    self.bitstring = BitArray()
                    self.bitstring.frombytes(b)

    def getbit(self, pos):
        if self.is_sparse():
            if pos in self._bit_1_indexes():
                return 1
            else:
                return 0
        else:
            try:
                return int(self.bitstring[pos])
            except IndexError:
                return 0

    @property
    def bytes(self):
        return b''.join([self.meta.tobytes(), self.bitstring.tobytes()])

    @property
    def bin(self):
        return ''.join([self.meta.to01(), self.bitstring.to01()])

    def choose_optimal_encoding(self, colour=None):
        colours = self.colours()
        if colours:
            if colour:
                byte_order = choose_int_encoding([colour])
            else:
                byte_order = choose_int_encoding(colours)
            sparse_byte_length = byte_order*len(colours)
            dense_byte_length = max(colours)/8
            if dense_byte_length < sparse_byte_length:
                self.to_dense()
            else:
                self.to_sparse()
        else:
            self.to_sparse()
