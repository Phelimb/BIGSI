from bitarray import bitarray


class BitArray(bitarray):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
        indexes = []
        i = 0
        while True:
            try:
                i = self.index(True, i)
                indexes.append(i)
                i += 1
            except ValueError:
                break
        return indexes

    def colours(self):
        return self.indexes()
