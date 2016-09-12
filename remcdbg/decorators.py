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


def kmers_or_bytes(self, kmers):
    if self.compress_kmers and isinstance(kmers, list):
        return [self._kmer_to_bytes(k) for k in kmers]
    elif self.compress_kmers:
        return self._kmer_to_bytes(kmers)
    else:
        return kmers


def convert_kmers(func):
    # Wrapper for functions of the form (self, kmers, colour, min_lexo=False)
    # or (self, kmers, min_lexo=False) and returns only the min of the kmer
    # and it's reverse complement
    @wraps(func)
    def convert_kmers_inner(self, kmers, *args, **kwargs):
        convert_func = choose_convert_func(kmers)
        # Are the kmers already converted
        if not kwargs.get('min_lexo'):
            # It is a list of kmers or a single kmer?
            kmers = convert_func(kmers)
        kmers = kmers_or_bytes(self, kmers)
        return func(self, kmers, *args, **kwargs)
    return convert_kmers_inner
