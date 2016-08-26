from __future__ import print_function
import redis
import math
from pathos.multiprocessing import ProcessPool as Pool
# pool = Pool()
# set up redis connections
KMER_SHARDING = {}

KMER_SHARDING[0] = {0: ''}
KMER_SHARDING[1] = {0: 'A', 1: 'T', 2: 'C', 3: 'G'}
KMER_SHARDING[2] = {0: 'AA', 1: 'AT', 2: 'AC', 3: 'AG', 4: 'TA', 5: 'TT', 6: 'TC',
                    7: 'TG', 8: 'CA', 9: 'CT', 10: 'CC', 11: 'CG', 12: 'GA', 13: 'GT', 14: 'GC', 15: 'GG'}
KMER_SHARDING[3] = {0: 'AAA', 1: 'AAT', 2: 'AAC', 3: 'AAG', 4: 'ATA', 5: 'ATT', 6: 'ATC', 7: 'ATG', 8: 'ACA', 9: 'ACT', 10: 'ACC', 11: 'ACG', 12: 'AGA', 13: 'AGT', 14: 'AGC', 15: 'AGG', 16: 'TAA', 17: 'TAT', 18: 'TAC', 19: 'TAG', 20: 'TTA', 21: 'TTT', 22: 'TTC', 23: 'TTG', 24: 'TCA', 25: 'TCT', 26: 'TCC', 27: 'TCG', 28: 'TGA', 29: 'TGT', 30: 'TGC', 31:
                    'TGG', 32: 'CAA', 33: 'CAT', 34: 'CAC', 35: 'CAG', 36: 'CTA', 37: 'CTT', 38: 'CTC', 39: 'CTG', 40: 'CCA', 41: 'CCT', 42: 'CCC', 43: 'CCG', 44: 'CGA', 45: 'CGT', 46: 'CGC', 47: 'CGG', 48: 'GAA', 49: 'GAT', 50: 'GAC', 51: 'GAG', 52: 'GTA', 53: 'GTT', 54: 'GTC', 55: 'GTG', 56: 'GCA', 57: 'GCT', 58: 'GCC', 59: 'GCG', 60: 'GGA', 61: 'GGT', 62: 'GGC', 63: 'GGG'}


def execute_pipeline(p):
    p.execute()


class McDBG(object):

    def __init__(self, ports):
        # colour
        self.ports = ports
        self.sharding_level = int(math.log(len(ports), 4))
        assert len(ports) in [0, 4, 64]
        self.connections = {}
        self._create_connections()

    def _create_connections(self):
        # kmers stored in DB 2
        # colour arrays in DB 1
        # stats in DB 0
        self.connections['stats'] = {}
        self.connections['colours'] = {}
        self.connections['kmers'] = {}
        for i, port in enumerate(self.ports):
            self.connections['stats'][i] = redis.StrictRedis(
                host='localhost', port=port, db=0)
            # self.connections['colours'][i] = redis.StrictRedis(
            #     host='localhost', port=port, db=1)
            kmer_key = KMER_SHARDING[self.sharding_level][i]
            self.connections['kmers'][kmer_key] = redis.StrictRedis(
                host='localhost', port=port, db=2)

    def _create_kmer_pipeline(self):
        # kmers stored in DB 2
        # colour arrays in DB 1
        # stats in DB 0
        pipelines = {}
        for i, port in enumerate(self.ports):
            kmer_key = KMER_SHARDING[self.sharding_level][i]
            pipelines[kmer_key] = self.connections[
                'kmers'][kmer_key].pipeline(transaction=False)
        return pipelines

    def _execute_pipeline(self, pipelines):
        # pool = Pool(max((len(self.ports), 1)))
        # results = pool.map(execute_pipeline, pipelines.values())
        # # close the pool and wait for the work to finish
        # pool.close()
        # pool.join()
        return [p.execute() for p in pipelines.values()]

    def set_kmer(self, kmer, colour):
        self.connections['kmers'][
            kmer[:self.sharding_level]].setbit(kmer, colour, 1)

    def set_kmers(self, kmers, colour):
        pipelines = self._create_kmer_pipeline()
        [pipelines[kmer[:self.sharding_level]].setbit(
            kmer, colour, 1) for kmer in kmers]
        self._execute_pipeline(pipelines)

    # def set_colour(self, ckey, colour, v=1):
    #     shard = ckey % len(self.ports)
    #     self.connections['colours'][shard].setbit(ckey, colour, v)

    def delete(self):
        for k, v in self.connections.items():
            for i, connection in v.items():
                connection.flushall()

    def add_sample(self, sample_name):
        existing_index = self.get_sample_colour(sample_name)
        if existing_index is not None:
            raise ValueError("%s already exists in the db" % sample_name)
        else:
            num_colours = self.sample_redis.get('num_colours')
            if num_colours is None:
                num_colours = 0
            else:
                num_colours = int(num_colours)
            pipe = self.sample_redis.pipeline()
            pipe.set('s%s' % sample_name, num_colours).incr('num_colours')
            pipe.execute()
            return num_colours
# >>> with r.pipeline() as pipe:
# ...     while 1:
# ...         try:
# ...             # put a WATCH on the key that holds our sequence value
# ...             pipe.watch('OUR-SEQUENCE-KEY')
# ...             # after WATCHing, the pipeline is put into immediate execution
# ...             # mode until we tell it to start buffering commands again.
# ...             # this allows us to get the current value of our sequence
# ...             current_value = pipe.get('OUR-SEQUENCE-KEY')
# ...             next_value = int(current_value) + 1
# ...             # now we can put the pipeline back into buffered mode with MULTI
# ...             pipe.multi()
# ...             pipe.set('OUR-SEQUENCE-KEY', next_value)
# ...             # and finally, execute the pipeline (the set command)
# ...             pipe.execute()
# ...             # if a WatchError wasn't raised during execution, everything
# ...             # we just did happened atomically.
# ...             break
# ...        except WatchError:
# ...             # another client must have changed 'OUR-SEQUENCE-KEY' between
# ...             # the time we started WATCHing it and the pipeline's execution.
# ...             # our best bet is to just retry.
# ...             continue

    def get_sample_colour(self, sample_name):
        return self.sample_redis.get('s%s' % sample_name)

    @property
    def sample_redis(self):
        return self.connections['stats'][0]

    @property
    def num_colours(self):
        try:
            return int(self.sample_redis.get('num_colours'))
        except TypeError:
            return 0

    def count_kmers(self):
        return sum([r.dbsize() for r in self.connections['kmers'].values()])

    def calculate_memory(self):
        memory = sum([r.info().get('used_memory')
                      for r in self.connections['kmers'].values()])
        self.connections['stats'][1].set([self.count_kmers()], memory)
        return memory
