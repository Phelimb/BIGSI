#! /usr/bin/env python
from __future__ import print_function
import sys
import os
import argparse
import redis
import json
sys.path.append(
    os.path.realpath(
        os.path.join(
            os.path.dirname(__file__),
            "..")))
from cbg.version import __version__
import logging
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)
from cbg.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)


import hug
import tempfile
from cbg.graph import CBG

BFSIZE = int(os.environ.get("BFSIZE", 25000000))
NUM_HASHES = int(os.environ.get("NUM_HASHES", 3))
CREDIS = bool(os.environ.get("CREDIS", True))
CELERY = bool(int(os.environ.get("CELERY", 0)))
# if CREDIS:
#     logger.info(
#         "You're running with credis.")
# if CELERY:
#     logger.info(
#         "You're running using celery background process. Please make sure celery is running in the background otherwise tasks may hang indefinitely ")
CONN_CONFIG = []
redis_envs = [env for env in os.environ if "REDIS" in env]
if len(redis_envs) == 0:
    CONN_CONFIG = [('localhost', 7000, 2)]
else:
    for i in range(int(len(redis_envs)/2)):
        hostname = os.environ.get("REDIS_IP_%s" % str(i + 1))
        port = int(os.environ.get("REDIS_PORT_%s" % str(i + 1)))
        CONN_CONFIG.append((hostname, port, 2))
from cbg.cmds.insert import insert
from cbg.cmds.search import search
# from cbg.cmds.stats import stats
from cbg.cmds.samples import samples
# from cbg.cmds.dump import dump
# from cbg.cmds.load import load
from cbg.cmds.delete import delete
from cbg.cmds.bloom import bloom
from cbg.cmds.build import build
from cbg.cmds.merge import merge
from cbg.cmds.rowjoin import rowjoin
# from cbg.cmds.bitcount import bitcount
# from cbg.cmds.jaccard_index import jaccard_index
from cbg.utils.cortex import GraphReader
from cbg.utils import seq_to_kmers
import cProfile
from cbg.version import __version__


def do_cprofile(func):
    def profiled_func(*args, **kwargs):
        profile = cProfile.Profile()
        try:
            profile.enable()
            result = func(*args, **kwargs)
            profile.disable()
            return result
        finally:
            profile.print_stats()
    return profiled_func


API = hug.API('cbg-%s' % str(__version__))
STORAGE = os.environ.get("STORAGE", 'berkeleydb')
BDB_DB_FILENAME = os.environ.get("BDB_DB_FILENAME", './db')
CACHESIZE = int(os.environ.get("CACHESIZE", 1))
# DEFAULT_GRAPH = GRAPH = Graph(BDB_DB_FILENAME)


DEFUALT_DB_DIRECTORY = "./db-cbg/"


def extract_kmers_from_ctx(ctx, k):
    gr = GraphReader(ctx)
    for i in gr:
        for kmer in seq_to_kmers(i.kmer.canonical_value, k):
            yield kmer


@hug.object(name='cbg', version='0.1.1', api=API)
@hug.object.urls('/', requires=())
class cbg(object):

    @hug.object.cli
    @hug.object.post('/init', output_format=hug.output_format.json)
    def init(self, db, k=31, m=25*10**6, h=3, force=False):
        cbg = CBG.create(db=db, k=k, m=m, h=h, force=force)
        return {'k': k, 'm': m, 'h': h, 'db': db}

    @hug.object.cli
    @hug.object.post('/insert', output_format=hug.output_format.json)
    def insert(self, bloom_filter):
        """Inserts a bloom filter into the graph

        e.g. cbg insert ERR1010211.bloom

        """
        graph = get_graph()
        result = insert(bloom_filter, async=CELERY)
        graph.sync()
        return {"result": result, 'took':
                float(hug_timer)}

    @hug.object.cli
    @hug.object.post('/bloom')
    def bloom(self, outfile, db=DEFUALT_DB_DIRECTORY, kmers=None, seqfile=None, ctx=None):
        cbg = CBG(db)
        """Creates a bloom filter from a sequence file or cortex graph. (fastq,fasta,bam,ctx)

        e.g. cbg insert ERR1010211.ctx

        """
        if ctx:
            kmers = extract_kmers_from_ctx(ctx, cbg.kmer_size)
        if not kmers and not seqfile:
            return "--kmers or --seqfile must be provided"
        bf = bloom(outfile=outfile, kmers=kmers, kmer_file=seqfile, graph=cbg)

    @hug.object.cli
    @hug.object.post('/build', output_format=hug.output_format.json)
    def build(self, db: hug.types.text,
              bloomfilters: hug.types.multiple,
              samples: hug.types.multiple = []):
        if samples:
            assert len(samples) == len(bloomfilters)
        else:
            samples = bloomfilters
        return build(graph=CBG(db), bloomfilter_filepaths=bloomfilters, samples=samples)

    @hug.object.cli
    @hug.object.get('/search', examples="seq=ACACAAACCATGGCCGGACGCAGCTTTCTGA",
                    output_format=hug.output_format.json,
                    response_headers={"Access-Control-Allow-Origin": "*"})
    # @do_cprofile
    def search(self, db: hug.types.text=None,
               seq: hug.types.text=None,
               seqfile: hug.types.text=None,
               threshold: hug.types.float_number=1.0,
               output_format: hug.types.one_of(("json", "tsv", "fasta"))='json',
               pipe_out: hug.types.smart_boolean=False,
               pipe_in: hug.types.smart_boolean=False,
               cachesize: hug.types.number=4,
               score: hug.types.smart_boolean=False):
        if db is None:
            db = BDB_DB_FILENAME
        cbg = CBG(db, cachesize=cachesize)
        """Returns samples that contain the searched sequence.
        Use -f to search for sequence from fasta"""
        if output_format in ["tsv", "fasta"]:
            pipe_out = True

        if not pipe_in and (not seq and not seqfile):
            return "-s or -f must be provided"
        if seq == "-" or pipe_in:
            _, fp = tempfile.mkstemp(text=True)
            with open(fp, 'w') as openfile:
                for line in sys.stdin:
                    openfile.write(line)
            result = search(
                seq=None, fasta_file=fp, threshold=threshold,
                graph=cbg,
                output_format=output_format,
                pipe=pipe_out,
                score=score)

        else:
            result = search(seq=seq,
                            fasta_file=seqfile,
                            threshold=threshold,
                            graph=cbg,
                            output_format=output_format,
                            pipe=pipe_out,
                            score=score)

        if not pipe_out:
            return result

    @hug.object.cli
    @hug.object.delete('/', output_format=hug.output_format.json)
    def delete(self, db: hug.types.text=None):
        try:
            cbg = CBG(db)
        except ValueError:
            pass
        else:
            return delete(cbg)

    # @hug.object.cli
    # @hug.object.get('/graph', output_format=hug.output_format.json)
    # def stats(self):
    #     return stats(graph=get_graph())

    @hug.object.cli
    @hug.object.get('/samples', output_format=hug.output_format.json)
    def samples(self, sample_name: hug.types.text=None, db: hug.types.text=None, delete: hug.types.smart_boolean=False):
        return samples(sample_name, graph=get_graph(bdb_db_filename=db), delete=delete)

    # @hug.object.cli
    # @hug.object.post('/dump', output_format=hug.output_format.json)
    # def dump(self, filepath):
    #     r = dump(graph=get_graph(), file=filepath)
    #     return r

    # @hug.object.cli
    # @hug.object.post('/load', output_format=hug.output_format.json)
    # def load(self, filepath):
    #     r = load(graph=get_graph(), file=filepath)
    #     return r


def main():
    API.cli()

if __name__ == "__main__":
    main()
