import redis
from cbg.utils import chunks


class HyperLogLogJaccardIndex(object):

    def __init__(self, host='localhost', port='6379', db=0, prefix="hll_"):
        self.storage = redis.StrictRedis(host=host, port=port, db=int(db))
        self.prefix = prefix
        self.max_N_pfadd = 100000

    def insert(self, kmers, sample):
        if isinstance(kmers, str):
            return self._massive_insert([kmers], sample)
        else:
            return self._massive_insert(kmers, sample)

    def _massive_insert(self, kmers, sample):
        for _kmers in chunks(kmers, self.max_N_pfadd):
            self.storage.pfadd(self._storage_key(sample), *_kmers)

    def union(self, sample1, sample2):
        samples = [self._storage_key(sample1), self._storage_key(sample2)]
        self.storage.pfcount(*samples)
        return self.storage.pfcount(*samples)

    def count(self, *args):
        if not args:
            return 0
        samples = [self._storage_key(s) for s in args]
        return self.storage.pfcount(*samples)

    def _storage_key(self, sample):
        return '%s%s' % (self.prefix, sample)

    def intersection(self, sample1, sample2):
        count1 = self.count(sample1)
        count2 = self.count(sample2)
        union = self.union(sample1, sample2)
        # http://dsinpractice.com/2015/09/07/counting-unique-items-fast-unions-and-intersections/
        intersection = count1+count2-union
        return intersection

    def jaccard_index(self, sample1, sample2):
        union = self.union(sample1, sample2)
        # http://dsinpractice.com/2015/09/07/counting-unique-items-fast-unions-and-intersections/
        intersection = self.intersection(sample1, sample2)
        return intersection/float(union)

    def jaccard_distance(self, sample1, sample2):
        union = self.union(sample1, sample2)
        # http://dsinpractice.com/2015/09/07/counting-unique-items-fast-unions-and-intersections/
        intersection = self.intersection(sample1, sample2)
        return (union-intersection)/float(union)

    def symmetric_difference(self, sample1, sample2):
        union = self.union(sample1, sample2)
        intersection = self.intersection(sample1, sample2)
        return union-intersection

    def difference(self, sample1, sample2):
        count1 = self.count(sample1)
        intersection = self.intersection(sample1, sample2)
        return count1-intersection

    def delete_all(self):
        self.storage.flushall()
