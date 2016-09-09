from __future__ import print_function
from functools import wraps
from remcdbg.utils import convert_query_kmers
from remcdbg.utils import convert_query_kmer


def choose_convert_func(kmers):
    if not isinstance(kmers, list):
        convert_func = convert_query_kmer
    else:
        convert_func = convert_query_kmers
    return convert_func


def choose_return_func(self, func, kmers, colour, min_lexo):
    if colour is not None:
        return func(self, kmers, colour, min_lexo)
    else:
        return func(self, kmers, min_lexo)


def convert_kmers(func):
    # Wrapper for functions of the form (self, kmers, colour, min_lexo=False)
    # or (self, kmers, min_lexo=False) and returns only the min of the kmer
    # and it's reverse complement
    @wraps(func)
    def inner(self, kmers, colour=None, min_lexo=False):
        convert_func = choose_convert_func(kmers)
        # Are the kmers already converted
        if not min_lexo:
            # It is a list of kmers or a single kmer?
            kmers = convert_func(kmers)
        return choose_return_func(self, func, kmers, colour, min_lexo)
    return inner
