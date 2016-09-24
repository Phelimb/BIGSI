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
from remcdbg.version import __version__

CONN_CONFIG = []
redis_envs = [env for env in os.environ if "REDIS" in env]
if len(redis_envs) == 0:
    CONN_CONFIG = [('localhost', 6379)]
else:
    for i in range(int(len(redis_envs)/2)):
        hostname = os.environ.get("REDIS_IP_%s" % str(i + 1))
        port = int(os.environ.get("REDIS_PORT_%s" % str(i + 1)))
        CONN_CONFIG.append((hostname, port))


def run_subtool(parser, args):
    if args.command == 'insert':
        from remcdbg.cmds.insert import run
    elif args.command == "query":
        from remcdbg.cmds.query import run
    elif args.command == "stats":
        from remcdbg.cmds.stats import run
    elif args.command == "samples":
        from remcdbg.cmds.samples import run
    elif args.command == "dump":
        from remcdbg.cmds.dump import run
    elif args.command == "compress":
        from remcdbg.cmds.compress import run
    elif args.command == "uncompress":
        from remcdbg.cmds.uncompress import run
    elif args.command == "shutdown":
        from remcdbg.cmds.shutdown import run
    elif args.command == "bitcount":
        from remcdbg.cmds.bitcount import run
    else:
        parser.print_help()
    # run the chosen submodule.
    try:
        run(parser, args, CONN_CONFIG)
    except (UnboundLocalError, redis.exceptions.BusyLoadingError) as e:
        if e is isinstance(e, redis.exceptions.BusyLoadingError):
            print(
                "Redis is loading the dataset in memory. Please try again when finished. ")


class ArgumentParserWithDefaults(argparse.ArgumentParser):

    def __init__(self, *args, **kwargs):
        super(ArgumentParserWithDefaults, self).__init__(*args, **kwargs)
        self.add_argument(
            "-q",
            "--quiet",
            help="do not output warnings to stderr",
            action="store_true",
            dest="quiet")


def main():
    #########################################
    # create the top-level parser
    #########################################
    parser = argparse.ArgumentParser(
        prog='remcdbg',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--version", help="atlas version",
                        action="version",
                        version="%(prog)s " + str(__version__))
    parser.set_defaults(func=run_subtool)

    subparsers = parser.add_subparsers(
        title='[sub-commands]',
        dest='command',
        parser_class=ArgumentParserWithDefaults)

    db_parser_mixin = argparse.ArgumentParser(add_help=False)
    compress_mixin = argparse.ArgumentParser(add_help=False)
    compress_mixin.add_argument(
        '--sparsity_threshold', type=float, default=0.05, help="Default=0.05")

    ##########
    # Insert
    ##########
    parser_insert = subparsers.add_parser(
        'insert',
        help='adds a set of kmers to the DB',
        parents=[db_parser_mixin])
    parser_insert.add_argument('kmer_file', type=str, help='kmer_file')
    parser_insert.add_argument("--sample_name", required=False)
    parser_insert.set_defaults(func=run_subtool)

    ##########
    # Query
    ##########
    parser_query = subparsers.add_parser(
        'query',
        help='querys a fasta against the DB',
        parents=[db_parser_mixin])
    parser_query.add_argument("fasta", type=str, help='fastafile')
    parser_query.add_argument("--threshold", type=int,
                              help='One show results that have this %% identity. Default:100%', default=100)
    parser_query.set_defaults(func=run_subtool)

    ##########
    # Stats
    ##########
    parser_stats = subparsers.add_parser(
        'stats',
        help='adds a set of kmers to the DB',
        parents=[db_parser_mixin])
    parser_stats.set_defaults(func=run_subtool)

    parser_samples = subparsers.add_parser(
        'samples',
        help='Colour to sample ID',
        parents=[db_parser_mixin])
    parser_samples.set_defaults(func=run_subtool)

    parser_compress = subparsers.add_parser(
        'compress',
        help='Compresses the database',
        parents=[db_parser_mixin, compress_mixin])
    parser_compress.set_defaults(func=run_subtool)

    parser_uncompress = subparsers.add_parser(
        'uncompress',
        help='Uncompresses the database',
        parents=[db_parser_mixin, compress_mixin])
    parser_uncompress.set_defaults(func=run_subtool)

    parser_dump = subparsers.add_parser(
        'dump',
        help='Print kmer colour matrix',
        parents=[db_parser_mixin])
    parser_dump.set_defaults(func=run_subtool)

    parser_bitcount = subparsers.add_parser(
        'bitcount',
        help='What is the distribution of bitcounts',
        parents=[db_parser_mixin])
    parser_bitcount.set_defaults(func=run_subtool)
    ##
    parser_shutdown = subparsers.add_parser(
        'shutdown',
        help='shutsdown all the redis instances',
        parents=[db_parser_mixin])
    parser_shutdown.set_defaults(func=run_subtool)

    args = parser.parse_args()

    args.func(parser, args)

if __name__ == "__main__":
    main()
