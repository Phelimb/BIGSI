#! /usr/bin/env python
"""
Conversion from a berkeleyDB v0.1 BIGSI to v0.3 berkeleyDB and v0.3 rocksDB
Requires v0.3 installed
"""

import sys
import bsddb3.db as db
import bitarray
import bigsi.version
from bigsi import BIGSI
from bigsi.matrix import BitMatrix
from bigsi.constants import DEFAULT_BERKELEY_DB_CONFIG
from bigsi.storage import get_storage
from bigsi.graph.metadata import SampleMetadata

import collections
assert int(bigsi.version.__version__[3])==3


def get_rows(in_db, bloom_filter_size):
    for i in range(bloom_filter_size):
        if (i % (bloom_filter_size/100))==0:
            print(i, 100*i/bloom_filter_size)
        key = str.encode(str(i))
        key = (i).to_bytes(4, byteorder='big')
        val=bitarray.bitarray()
        val.frombytes(in_db[key])
        yield val

BLOOMFILTER_SIZE_KEY = "ksi:bloomfilter_size"
NUM_HASH_FUNCTS_KEY = "ksi:num_hashes"

def main():
    infile = sys.argv[1]
    outfile = sys.argv[2]

    in_graph = db.DB()
    in_graph.set_cachesize(4,0)
    in_graph.open(infile+"/graph", flags=db.DB_RDONLY)

    in_metadata = db.DB()
    in_metadata.set_cachesize(4,0)
    in_metadata.open(infile+"/metadata", flags=db.DB_RDONLY)
    num_samples=int.from_bytes(in_metadata[b'num_colours'], 'big')
    bloom_filter_size=int.from_bytes(in_metadata[b'bloom_filter_size'], 'big')
    kmer_size=int.from_bytes(in_metadata[b'kmer_size'], 'big')
    num_hashes=int.from_bytes(in_metadata[b'num_hashes'], 'big')

    ## Create the sample metadata
    colour_sample={}
    for k, v in in_metadata.items():
        if "colour" in k.decode("utf-8") and not k.decode("utf-8")=="num_colours":
            colour=k.decode("utf-8").split("colour")[1]
            sample=v.decode("utf-8")
            colour_sample[colour]=sample
    samples=list(collections.OrderedDict(sorted(colour_sample.items())).values())


    ## Add the sample metadata
    config={
    "storage-engine": "berkeleydb",
    "storage-config": {"filename": outfile},
    "k": 31, "m": 25 * 10 ** 6, "h": 3,
    }
    storage=get_storage(config) 
    sm = SampleMetadata(storage).add_samples(samples)
    ## Create the kmer signature index
    storage.set_integer(BLOOMFILTER_SIZE_KEY, bloom_filter_size)
    storage.set_integer(NUM_HASH_FUNCTS_KEY, num_hashes)
    BitMatrix.create(storage=storage,
        rows=get_rows(in_graph, bloom_filter_size), num_rows=bloom_filter_size, num_cols=num_samples)


    in_graph.close()
    in_metadata.close()

main()