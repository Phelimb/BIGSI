class BaseGraphStorage:

    def __init__(self, storage):
        self.storage = storage

    def insert(self, kmers, colour):
        raise NotImplementedError("Implemented in child")

    def lookup(self, kmer):
        raise NotImplementedError("Implemented in child")
