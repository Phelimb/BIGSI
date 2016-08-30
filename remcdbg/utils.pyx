COMPLEMENT = {'A': 'T', 'C': 'G', 'G': 'C', 'T': 'A'}


def reverse_comp(str s):
    return "".join([COMPLEMENT.get(base, base) for base in reversed(s)])


def min_lexo(str k):
    l = [k, reverse_comp(k)]
    l.sort()
    return l[0]

def seq_to_kmers(str seq):
    for i in range(len(seq)-31+1):
        yield seq[i:i+31]

def bits(f):
    return [(ord(s) >> i) & 1 for s in list(f) for i in xrange(7, -1, -1)]