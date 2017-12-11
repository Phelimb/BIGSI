# BItsliced Genomic Signature Index [BIGSI]
<!--[![Build Status](https://travis-ci.org/Phelimb/bigsi.svg)](https://travis-ci.org/Phelimb/bigsi)-->

BIGSI can search a collection of raw (fastq/bam), contigs or assembly for genes, variant alleles and arbitrary sequence. It can scale to millions of bacterial genomes requiring ~3MB of disk per sample while maintaining millisecond kmer queries in the collection.

This tool was formally named "Coloured Bloom Graph" or "CBG" in reference to the fact that it can be viewed as a coloured probabilistic de Bruijn graph.


# Installing without docker

bigsi has a docker image that bundles mccortex, berkeley DB and BIGSI in one image. Skip to `Quickstart with docker` for an easier install.. 

#### Install requirement berkeley-db

	brew install berkeley-db4
	pip install cython
	BERKELEYDB_DIR=/usr/local/opt/berkeley-db4/ pip install bsddb3

For berkeley-db install on unix, see [Dockerfile](Dockerfile). 

#### Install BIGSI

	pip install bigsi

## Quickstart

## Prepare the data

Requires [mccortex](github.com/mcveanlab/mccortex). 

	mccortex/bin/mccortex31 build -k 31 -s test1 -1 /data/kmers.txt /data/test1.ctx
	mccortex/bin/mccortex31 build -k 31 -s test2 -1 /data/kmers.txt /data/test2.ctx

#### Construct the bloom filters

	bigsi init test-data/db --k 31 --m 1000 --h 1

	bigsi bloom --db test-data/db -c test-data/test1.ctx test-data/test1.bloom
	bigsi bloom --db test-data/db -c test-data/test1.ctx test-data/test2.bloom
	
### Build the combined graph

	bigsi build test-data/db test-data/test1.bloom test-data/test2.bloom

### Query the graph
	bigsi search --db test-data/db -s CGGCGAGGAAGCGTTAAATCTCTTTCTGACG

	

# Quickstart with docker

	docker pull phelimb/bigsi
	docker run phelimb/bigsi bigsi --help
	
### Preparing your data

BIGSI using single colour graphs to construct the coloured graph. 
Use [mccortex](https://github.com/mcveanlab/mccortex) to build. 
	
	PWD=`pwd`
	docker run -v $PWD/test-data:/data phelimb/bigsi mccortex/bin/mccortex31 build -k 31 -s test1 -1 /data/kmers.txt /data/test1.ctx
	docker run -v $PWD/test-data:/data phelimb/bigsi mccortex/bin/mccortex31 build -k 31 -s test2 -1 /data/kmers.txt /data/test2.ctx

### Building a BIGSI

#### Construct the bloom filters

	docker run -v $PWD/test-data:/data phelimb/bigsi bigsi  init /data/test.bigsi --k 31 --m 1000 --h 1

	docker run -v $PWD/test-data:/data phelimb/bigsi bigsi bloom --db /data/test.bigsi -c /data/test1.ctx /data/test1.bloom	
	docker run -v $PWD/test-data:/data phelimb/bigsi bigsi bloom --db /data/test.bigsi -c /data/test1.ctx /data/test2.bloom	
#### Build the combined graph
	docker run -v $PWD/test-data:/data phelimb/bigsi bigsi build /data/test.bigsi /data/test1.bloom /data/test2.bloom

#### Query the graph
	docker run -v $PWD/test-data:/data phelimb/bigsi bigsi search --db /data/test.bigsi -s CGGCGAGGAAGCGTTAAATCTCTTTCTGACG
	


# Search for variant alleles

You'll need to install atlas-var e.g.

	pip install git+https://github.com/Phelimb/atlas-var.git

You can find instructions on how to generate probes for the variants that you want to genotype at [atlas-var](https://github.com/Phelimb/atlas-var.git)

e.g.
	
	cat example-data/kmers.fasta | ./bigsi/__main__.py search --pipe_in -o tsv

	atlas-var make-probes -v A1234T ../atlas-var/example-data/NC_000962.3.fasta | ./bigsi/__main__.py search - --pipe_in -o tsv

# Low memory builds

If you don't have enough RAM to hold the whole graph in memory you can build in chunks and merge.

e.g.

	ls path/to/bloom/filters/blooms/* | split -l 500

	mkdir -p tmp_bigsi/
	for i in xa*
	do
	    bigsi init tmp_bigsi/"$i" --k 31 --m 25000000 --h 1
	    bigsi build tmp_bigsi/"$i" $(cat "$i")
	done	

	bigsi merge merged_bigsi tmp_bigsi/xa*

# Parameter choices:


## How do I decide on bloom filter size and number of hashes when building a BIGSI? 

### Short answer:

Use an onli\ne calculator to determine the bloom filter size (m) and number hashes (k) to give a false positive rate of ~0.05 for the number of kmers your expect per sample. 

e.g. for a Ecoli sequence set I'd expect ~5million unique kmers so I'd set m=30,000,00 and h = 4. (see [http://hur.st/bloomfilter?n=5000000&p=0.05](http://hur.st/bloomfilter?n=5000000&p=0.05))

The resulting disk/memory size of the graph will be:

	~ N * m bits 
	
### Long answer: 

Your choices of parameters depends on: the number of samples (N) you're going to add,  the size of queries (length of the query sequence) that you'll be doing (Q) and your kmer size (k). 

Bloom filter false positive rate (p)


The FDR of any query can be calculated by:

	False discoveries (V) = p ^ (Q-k+1) * N
	FDR = V/R where R is the number of discoveries for a given query. 

p depends on your bloom filter size (m), number of hashes (h) and the total number of kmers per sample (Ns). You can optimise your choice of m and h based on the following formula:

	m = (Ns * log(p)) / log(1.0 / (pow(2.0, log(2.0))))
	k = log(2.0) * m / Ns

There are several [online calculators](http://hur.st/bloomfilter?n=5000000&p=0.5) that will help you with this choice. 

For example, if I expect my minimum expected query size to be 100base pairs and I'm building a k=31 graph of 10,000 samples my E[V] = 10000 * (p^(70)) then I can choose p to be 0.5 and still have an expected false discoveries per query (E[V]) ~= 10-^-18

However, if my minimum expected query size is 40 bps using the same parameters would yield E[V] ~= 10.0 which is likely unexceptable. However, if I decrease p to 0.05 my E[V] will decrease to ~ 10^-10. 

*Note* that you cannot change these parameters once samples have been added to the graph (you would need to rebuild from scratch). So, if in doubt choose parameters such that p <= 0.1. Choosing a low p will increase the size of the database but it's likely to not be prohibitive. You can calculate the resulting data structure size with:
	
	N * m bits 

<!--
## Accessing underlying bitmatrix

To iterate through the rows in the bitmatrix you can use this simple python3 script:

`python3 script.py db`

	"""
	script.py  - Iterate through the BloomFilterMatrix rows
	"""
	
	import sys
	import bsddb3.db as db
	import bitarray
	def main():
	    infile = sys.argv[1]
	
	    in_db = db.DB()
	    in_db.set_cachesize(4,0)
	    in_db.open(infile, flags=db.DB_RDONLY)
	
	    for i in range(25*10**6):
	        key = str.encode(str(i))
	        val=bitarray.bitarray()
	        val.frombytes(in_db[key])
	        print(i,val)
	    in_db.close()
	
	main()

-->

