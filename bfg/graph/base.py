

class BaseGraph(object):

    def __init__(self, kmer_size=31, binary_kmers=True, storage={'dict': None}):
        self.kmer_size = kmer_size
        self.binary_kmers = binary_kmers
        self.graph = None
        self.metadata = None
        self._choose_storage(storage)

    def insert(self, sample, kmers):
        raise NotImplementedError("Implemented in child classes")

    def lookup(self, kmer, num_elements=None):
        raise NotImplementedError("Implemented in child classes")

    def dump(self):
        raise NotImplementedError("Implemented in child classes")

    def load(self):
        raise NotImplementedError("Implemented in child classes")

    def dumps(self):
        raise NotImplementedError("Implemented in child classes")

    def loads(self):
        raise NotImplementedError("Implemented in child classes")

    def _choose_storage(self, storage):
        raise NotImplementedError("Implemented in child classes")

    def delete_all(self):
        self.graph.delete_all()
        self.metadata.delete_all()
        self.min_hash.delete_all()
