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
from atlasseq.version import __version__
import logging
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import hug

CONN_CONFIG = []
redis_envs = [env for env in os.environ if "REDIS" in env]
if len(redis_envs) == 0:
    CONN_CONFIG = [('localhost', 6379, 2)]
else:
    for i in range(int(len(redis_envs)/2)):
        hostname = os.environ.get("REDIS_IP_%s" % str(i + 1))
        port = int(os.environ.get("REDIS_PORT_%s" % str(i + 1)))
        CONN_CONFIG.append((hostname, port, 2))

from atlasseq.cmds.insert import insert
from atlasseq.cmds.query import query
from atlasseq.cmds.stats import stats
from atlasseq.cmds.samples import samples
from atlasseq.cmds.dump import dump
#from atlasseq.cmds.bitcount import bitcount
#from atlasseq.cmds.jaccard_index import jaccard_index


class ArgumentParserWithDefaults(argparse.ArgumentParser):

    def __init__(self, *args, **kwargs):
        super(ArgumentParserWithDefaults, self).__init__(*args, **kwargs)
        self.add_argument(
            "-q",
            "--quiet",
            help="do not output warnings to stderr",
            action="store_true",
            dest="quiet")

API = hug.API('seq')


@hug.object(name='seq', version='0.0.1', api=API)
@hug.object.urls('/', requires=())
class AtlasSeq(object):

    @hug.object.cli
    @hug.object.post('/insert')
    def insert(self, kmer_file, sample_name=None, force: hug.types.smart_boolean=False,
               intersect_kmers_file=None, count_only: hug.types.smart_boolean = False):
        logger.info("insert")
        return insert(
            kmer_file=kmer_file, conn_config=CONN_CONFIG, force=force, sample_name=sample_name,
            intersect_kmers_file=intersect_kmers_file, count_only=count_only)

    @hug.object.cli
    @hug.object.get('/search')
    def search(self, seq=None, fasta_file=None, threshold: hug.types.float_number=1.0):
        return query(seq=seq,
                     fasta_file=fasta_file, threshold=threshold, conn_config=CONN_CONFIG)

    @hug.object.cli
    @hug.object.get('/stats')
    def stats(self):
        return stats(conn_config=CONN_CONFIG)

    @hug.object.cli
    @hug.object.get('/samples')
    def samples(self):
        return samples(conn_config=CONN_CONFIG)

    @hug.object.cli
    @hug.object.get('/dump')
    def dump(self):
        return dump(conn_config=CONN_CONFIG)

    # @hug.object.cli
    # @hug.object.get('/bitcount')
    # def bitcount(self):
    #     return bitcount(conn_config=CONN_CONFIG)

    # @hug.object.cli
    # @hug.object.get('/js')
    # def distance(self, s1=None, s2=None):
    # return json.dumps(jaccard_index(s1, s2, conn_config=CONN_CONFIG),
    # indent=1)


def main():
    API.cli()

if __name__ == "__main__":
    main()
