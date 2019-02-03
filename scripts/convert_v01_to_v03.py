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


def convert_metadata(infile, config):
    in_metadata = db.DB()
    in_metadata.set_cachesize(4,0)
    in_metadata.open(infile+"/metadata", flags=db.DB_RDONLY)
    num_samples=int.from_bytes(in_metadata[b'num_colours'], 'big')
    bloom_filter_size=int.from_bytes(in_metadata[b'bloom_filter_size'], 'big')
    kmer_size=int.from_bytes(in_metadata[b'kmer_size'], 'big')
    num_hashes=int.from_bytes(in_metadata[b'num_hashes'], 'big')
    ## Create the sample metadata
    colour_sample={}
    for colour in range(num_samples):
        key="colour%i" % colour
        key=key.encode("utf-8")
        sample_name=in_metadata[key].decode('utf-8')
        colour_sample[colour]=sample_name
    print(colour_sample)
    ## Add the sample metadata

    storage=get_storage(config) 
    sm = SampleMetadata(storage)  
  
    for colour, sample_name in colour_sample.items():
        if "DELETED" in sample_name:
            print(colour, sample_name)
        sm._set_sample_colour(sample_name, colour)
        sm._set_colour_sample(colour, sample_name)
    sm._set_integer(sm.colour_count_key, num_samples)
    in_metadata.close()
    return num_samples

def convert_index(infile, config, num_samples):
    in_graph = db.DB()
    in_graph.set_cachesize(4,0)
    in_graph.open(infile+"/graph", flags=db.DB_RDONLY)

    # Create the kmer signature index
    storage=get_storage(config) 
    storage.set_integer(BLOOMFILTER_SIZE_KEY, config["m"])
    storage.set_integer(NUM_HASH_FUNCTS_KEY, config["h"])  
    BitMatrix.create(storage=storage,
        rows=get_rows(in_graph, config["m"]), num_rows=config["m"], num_cols=num_samples)
    in_graph.close()
    
def main():
    infile = sys.argv[1]
    outfile = sys.argv[2]
    config={
    "storage-engine": "berkeleydb",
    "storage-config": {"filename": outfile},
    "k": 31, "m": 25 * 10 ** 6, "h": 3,
    }    
    num_samples=convert_metadata(infile, config)
    convert_index(infile, config, num_samples)



main()