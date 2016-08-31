#! /usr/bin/env python
from __future__ import print_function
import sys
import os
import argparse
# from remcdbg.version import __version__
sys.path.append(
    os.path.realpath(
        os.path.join(
            os.path.dirname(__file__),
            "..")))


def run_subtool(parser, args):
    if args.command == 'insert':
        from remcdbg.cmds.insert import run
    elif args.command == "query":
        from remcdbg.cmds.query import run
    elif args.command == "stats":
        from remcdbg.cmds.stats import run
    # run the chosen submodule.
    run(parser, args)


def main():
    #########################################
    # create the top-level parser
    #########################################
    parser = argparse.ArgumentParser(
        prog='remcdbg',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # parser.add_argument("--version", help="atlas version",
    #                     action="version",
    #                     version="%(prog)s " + str(__version__))
    subparsers = parser.add_subparsers(
        title='[sub-commands]',
        dest='command',
        parser_class=argparse.ArgumentParser)

    db_parser_mixin = argparse.ArgumentParser(add_help=False)
    db_parser_mixin.add_argument("--ports", type=int, nargs='+')

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
    parser_query.set_defaults(func=run_subtool)

    ##########
    # Insert
    ##########
    parser_stats = subparsers.add_parser(
        'stats',
        help='adds a set of kmers to the DB',
        parents=[db_parser_mixin])
    parser_stats.set_defaults(func=run_subtool)
    ##
    args = parser.parse_args()
    args.func(parser, args)

if __name__ == "__main__":
    main()
