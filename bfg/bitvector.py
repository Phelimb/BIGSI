from bitarray import bitarray
import numpy as np


class BitArray(bitarray):

    def __init__(self, *args, **kwargs):
        super().__init__()

    def setbit(self, i, bit):
        if i < 0:
            raise ValueError("Index must be >= 0")
        try:
            self[i] = bit
            return self
        except IndexError:
            self.extend([False]*(1+i-self.length()))
            return self.setbit(i, bit)

    def getbit(self, i):
        try:
            return self[i]
        except IndexError:
            return False

    def indexes(self):
        return np.where(self)[0].tolist()

    def colours(self):
        return self.indexes()
