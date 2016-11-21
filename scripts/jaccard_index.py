#! /usr/bin/env python
import begin


def load_all_kmers(f):
    kmers = []
    with open(f, 'r') as inf:
        for line in inf:
            kmer = line.strip()

            kmers.append(kmer)
    return set(kmers)


@begin.start
def run(f1, f2):
    s1 = load_all_kmers(f1)
    s2 = load_all_kmers(f2)
    print(len(s1 & s2) / len(s1 | s2))
