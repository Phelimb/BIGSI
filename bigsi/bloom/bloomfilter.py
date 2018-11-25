import mmh3
from bitarray import bitarray


def _hash(element, seed, m):
    return mmh3.hash(element, seed) % m


def generate_hashes(element, number_hash_functions, bloomfilter_size):
    hashes = {
        _hash(element, seed, bloomfilter_size) for seed in range(number_hash_functions)
    }
    return hashes


class BloomFilter(object):
    def __init__(self, m, h):
        self.m = m
        self.h = h
        self.bitarray = bitarray(self.m)

    def __hashes(self, element):
        return generate_hashes(element, self.h, self.m)

    def add(self, e):
        for i in self.__hashes(e):
            self.bitarray[i] = True

    def update(self, elements):
        for e in list(elements):
            self.add(e)
        return self


def load_bitarray(f):
    bloomfilter = bitarray()
    with open(f, "rb") as inf:
        bloomfilter.fromfile(inf)
    return bloomfilter
