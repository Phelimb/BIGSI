#! /usr/bin/env python
from __future__ import print_function
import sys
import os
import io
import csv
import argparse
import redis
import json
import math
import logging
import hug
import tempfile
import humanfriendly
import yaml
import copy
import multiprocessing

from pyfasta import Fasta
from bigsi.version import __version__
from bigsi.graph import BIGSI

from bigsi.cmds.insert import insert
from bigsi.cmds.delete import delete
from bigsi.cmds.bloom import bloom
from bigsi.cmds.build import build
from bigsi.cmds.merge import merge
from bigsi.cmds.variant_search import BIGSIVariantSearch
from bigsi.cmds.variant_search import BIGSIAminoAcidMutationSearch

from bigsi.storage import get_storage

from bigsi.utils.cortex import extract_kmers_from_ctx
from bigsi.utils import seq_to_kmers
from bigsi.constants import DEFAULT_CONFIG

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def d_to_csv(d, with_header=True, carriage_return=True):
    df = []
    results = d["results"]
    if results:
        header = sorted(results[0].keys())
        if with_header:
            df.append(["query"] + header)

    for res in results:
        row = [d["query"]]
        for key in header:
            row.append(res[key])
        df.append(row)

    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
    for row in df:
        writer.writerow(row)
    csv_string = output.getvalue()
    if carriage_return:
        return csv_string
    else:
        return csv_string[:-1]


def search_bigsi(bigsi, seq, threshold, score):
    return {
        "query": seq,
        "threshold": threshold,
        "results": bigsi.search(seq, threshold, score),
        "citation": "http://dx.doi.org/10.1038/s41587-018-0010-1",
    }


def search_bigsi_parallel(l):
    bigsi = BIGSI(l[0][0])
    results = []
    for _, seq, threshold, score in l:
        results.append(search_bigsi(bigsi, seq, threshold, score))
    return results


API = hug.API("bigsi-%s" % str(__version__))


def get_config_from_file(config_file):
    if config_file is None:
        if os.environ.get("BIGSI_CONFIG"):
            config_file = os.environ.get("BIGSI_CONFIG")
        else:
            return DEFAULT_CONFIG
    with open(config_file, "r") as infile:
        config = yaml.load(infile, Loader=yaml.FullLoader)
    return config


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


@hug.object(name="bigsi", version="0.1.1", api=API)
@hug.object.urls("/", requires=())
class bigsi(object):
    @hug.object.cli
    @hug.object.post("/insert", output_format=hug.output_format.pretty_json)
    def insert(self, config: hug.types.text, bloomfilter, sample):
        """Inserts a bloom filter into the graph

        e.g. bigsi insert ERR1010211.bloom ERR1010211

        """
        config = get_config_from_file(config)
        index = BIGSI(config)
        return insert(index=index, bloomfilter=bloomfilter, sample=sample)

    @hug.object.cli
    @hug.object.post("/bloom")
    def bloom(self, ctx, outfile, config=None):
        """Creates a bloom filter from a sequence file or cortex graph. (fastq,fasta,bam,ctx)

        e.g. index insert ERR1010211.ctx

        """
        config = get_config_from_file(config)
        bf = bloom(
            config=config,
            outfile=outfile,
            kmers=extract_kmers_from_ctx(ctx, config["k"]),
        )

    @hug.object.cli
    @hug.object.post("/build", output_format=hug.output_format.pretty_json)
    def build(
        self,
        bloomfilters: hug.types.multiple = [],
        samples: hug.types.multiple = [],
        from_file: hug.types.text = None,
        config: hug.types.text = None,
    ):
        config = get_config_from_file(config)

        if from_file and bloomfilters:
            raise ValueError(
                "You can only specify blooms via from_file or bloomfilters, but not both"
            )
        elif from_file:
            samples = []
            bloomfilters = []
            with open(from_file, "r") as tsvfile:
                reader = csv.reader(tsvfile, delimiter="\t")
                for row in reader:
                    bloomfilters.append(row[0])
                    samples.append(row[1])
        if samples:
            assert len(samples) == len(bloomfilters)
        else:
            samples = bloomfilters

        if config.get("max_build_mem_bytes"):
            max_memory_bytes = humanfriendly.parse_size(config["max_build_mem_bytes"])
        else:
            max_memory_bytes = None

        return build(
            config=config,
            bloomfilter_filepaths=bloomfilters,
            samples=samples,
            max_memory=max_memory_bytes,
        )

    @hug.object.cli
    @hug.object.post("/merge", output_format=hug.output_format.pretty_json)
    def merge(self, config: hug.types.text, merge_config: hug.types.text):
        config = get_config_from_file(config)
        merge_config = get_config_from_file(merge_config)
        index1 = BIGSI(config)
        index2 = BIGSI(merge_config)
        merge(index1, index2)
        return {"result": "merged %s into %s." % (merge_config, config)}

    @hug.object.cli
    @hug.object.post(
        "/search",
        response_headers={"Access-Control-Allow-Origin": "*"},
        output=hug.output_format.text,
    )
    @hug.object.get(
        "/search",
        examples="seq=ACACAAACCATGGCCGGACGCAGCTTTCTGA",
        response_headers={"Access-Control-Allow-Origin": "*"},
        output=hug.output_format.text,
    )
    def search(
        self,
        seq: hug.types.text,
        threshold: hug.types.float_number = 1.0,
        config: hug.types.text = None,
        score: hug.types.smart_boolean = False,
        format: hug.types.one_of(["json", "csv"]) = "json",
    ):
        config = get_config_from_file(config)
        bigsi = BIGSI(config)
        d = search_bigsi(bigsi, seq, threshold, score)
        if format == "csv":
            return d_to_csv(d)
        else:
            return json.dumps(d, indent=4)

    @hug.object.cli
    @hug.object.post(
        "/variant_search",
        response_headers={"Access-Control-Allow-Origin": "*"},
        output=hug.output_format.text,
    )
    @hug.object.get(
        "/variant_search",
        response_headers={"Access-Control-Allow-Origin": "*"},
        output=hug.output_format.text,
    )
    def variant_search(
        self,
        reference: hug.types.text,
        ref: hug.types.text,
        pos: hug.types.number,
        alt: hug.types.text,
        gene: hug.types.text = None,
        genbank: hug.types.text = None,
        config: hug.types.text = None,
        format: hug.types.one_of(["json", "csv"]) = "json",
    ):
        config = get_config_from_file(config)
        bigsi = BIGSI(config)
        if genbank and gene:
            d = BIGSIAminoAcidMutationSearch(bigsi, reference, genbank).search(
                gene, ref, pos, alt
            )
        elif genbank or gene:
            raise ValueError("genbank and gene must be supplied together")
        else:
            d = BIGSIVariantSearch(bigsi, reference).search(ref, pos, alt)
        d["citation"] = "http://dx.doi.org/10.1038/s41587-018-0010-1"
        if format == "csv":
            return d_to_csv(d)
        else:
            return json.dumps(d, indent=4)

    @hug.object.cli
    @hug.object.post(
        "/bulk_search",
        response_headers={"Access-Control-Allow-Origin": "*"},
        output=hug.output_format.text,
    )
    @hug.object.get(
        "/bulk_search",
        examples="seqfile=query.fasta",
        response_headers={"Access-Control-Allow-Origin": "*"},
        output=hug.output_format.text,
    )
    def bulk_search(
        self,
        fasta: hug.types.text,
        threshold: hug.types.float_number = 1.0,
        config: hug.types.text = None,
        score: hug.types.smart_boolean = False,
        format: hug.types.one_of(["json", "csv"]) = "json",
        stream: hug.types.smart_boolean = False,
    ):
        config = get_config_from_file(config)

        fasta = Fasta(fasta)
        if not stream:
            _config = copy.copy(config)
            _config["nproc"] = 1
            csv_combined = ""
            nproc = config.get("nproc", 1)
            with multiprocessing.Pool(processes=nproc) as pool:
                args = [(_config, str(seq), threshold, score) for seq in fasta.values()]
                dd = pool.map_async(
                    search_bigsi_parallel, chunks(args, math.ceil(len(args) / nproc))
                ).get()
                dd = [item for sublist in dd for item in sublist]
            if format == "csv":
                return "\n".join([d_to_csv(d, False, False) for d in dd])
            else:
                return json.dumps(dd, indent=4)
        else:
            bigsi = BIGSI(config)
            csv_combined = ""
            for i, seq in enumerate(fasta.values()):
                seq = str(seq)
                d = {
                    "query": seq,
                    "threshold": threshold,
                    "results": bigsi.search(seq, threshold, score),
                    "citation": "http://dx.doi.org/10.1038/s41587-018-0010-1",
                }
                if format == "csv":
                    if i == 0:
                        with_header = True
                        carriage_return = False
                    elif i == len(fasta) - 1:
                        carriage_return = True
                    else:
                        with_header = False
                        carriage_return = False
                    csv_result = d_to_csv(d, with_header, carriage_return)
                    csv_combined += csv_result
                    if stream:
                        print(csv_result)
                else:
                    if stream:
                        print(json.dumps(d))

    @hug.object.cli
    @hug.object.delete("/", output_format=hug.output_format.pretty_json)
    def delete(self, config: hug.types.text = None):
        config = get_config_from_file(config)
        get_storage(config).delete_all()


def main():
    API.cli()


if __name__ == "__main__":
    main()
