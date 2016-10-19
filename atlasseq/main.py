#! /usr/bin/env python
from __future__ import print_function
import sys
import os
import argparse
import redis
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
    CONN_CONFIG = [('localhost', 6379)]
else:
    for i in range(int(len(redis_envs)/2)):
        hostname = os.environ.get("REDIS_IP_%s" % str(i + 1))
        port = int(os.environ.get("REDIS_PORT_%s" % str(i + 1)))
        CONN_CONFIG.append((hostname, port))

from atlasseq.cmds.insert import insert
from atlasseq.cmds.query import query
from atlasseq.cmds.stats import stats
from atlasseq.cmds.samples import samples
from atlasseq.cmds.dump import dump
from atlasseq.cmds.compress import compress
from atlasseq.cmds.uncompress import uncompress
from atlasseq.cmds.shutdown import shutdown
from atlasseq.cmds.bitcount import bitcount
from atlasseq.cmds.jaccard_index import jaccard_index


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
    def insert(self, kmer_file, sample_name=None):
        logger.info("insert")
        return insert(
            kmer_file=kmer_file, conn_config=CONN_CONFIG, sample_name=sample_name)

    @hug.object.cli
    @hug.object.get('/search')
    def search(self, fasta_file, threshold: hug.types.float_number=1.0):
        return query(
            fasta_file=fasta_file, threshold=threshold, conn_config=CONN_CONFIG)

    @hug.object.cli
    @hug.object.get('/stats')
    def stats(self):
        return stats(conn_config=CONN_CONFIG)

    @hug.object.cli
    @hug.object.get('/js')
    def distance(self, s1, s2):
        return jaccard_index(s1, s2, conn_config=CONN_CONFIG)

    @hug.object.cli
    @hug.object.get('/samples')
    def samples(self):
        return samples(conn_config=CONN_CONFIG)

    @hug.object.cli
    @hug.object.get('/compress')
    def compress(self):
        return compress(conn_config=CONN_CONFIG)

    @hug.object.cli
    @hug.object.get('/uncompress')
    def uncompress(self):
        return uncompress(conn_config=CONN_CONFIG)

    @hug.object.cli
    @hug.object.get('/dump')
    def dump(self):
        return dump(conn_config=CONN_CONFIG)

    @hug.object.cli
    @hug.object.get('/bitcount')
    def bitcount(self):
        return bitcount(conn_config=CONN_CONFIG)

    @hug.object.cli
    def shutdown(self):
        return shutdown(conn_config=CONN_CONFIG)


if __name__ == "__main__":
    API.cli()
