import hashlib
import struct
import sys
import logging
from functools import reduce
import numpy as np
from itertools import islice, chain

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")

COMPLEMENT = {"A": "T", "C": "G", "G": "C", "T": "A"}
BITS = {"A": "00", "G": "01", "C": "10", "T": "11"}
BASES = {"00": "A", "01": "G", "10": "C", "11": "T"}


def batch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([next(batchiter)], batchiter)


def bitwise_and(bitarrays):
    return reduce(lambda x, y: x & y, bitarrays)


def non_zero_bitarrary_positions(bitarray):
    return np.where(bitarray)[0].tolist()


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


def reverse_comp(s):
    return "".join([COMPLEMENT.get(base, base) for base in reversed(s)])


def convert_query_kmers(kmers):
    for k in kmers:
        yield convert_query_kmer(k)


def convert_query_kmer(kmer):
    return canonical(kmer)


def canonical(k):
    l = [k, reverse_comp(k)]
    l.sort()
    return l[0]


def min_lexo(k):
    l = [k, reverse_comp(k)]
    l.sort()
    return l[0]


def seq_to_kmers(seq, kmer_size):
    for i in range(len(seq) - kmer_size + 1):
        yield seq[i : i + kmer_size]
