# BItsliced Genomic Signature Index [BIGSI]
<!--[![Build Status](https://travis-ci.org/Phelimb/bigsi.svg)](https://travis-ci.org/Phelimb/bigsi)-->

BIGSI can search a collection of raw (fastq/bam), contigs or assembly for genes, variant alleles and arbitrary sequence. It can scale to millions of bacterial genomes requiring ~3MB of disk per sample while maintaining millisecond kmer queries in the collection.

This tool was formally named "Coloured Bloom Graph" or "CBG" in reference to the fact that it can be viewed as a coloured probabilistic de Bruijn graph.


Documentation can be found at https://bigsi.readme.io/. 
An index of the microbial ENA/SRA (Dec 2016) can be queried at http://www.bigsi.io. 

You can read more in our preprint here: https://www.biorxiv.org/content/early/2017/12/15/234955.

# Install

bigsi has a docker image that bundles mccortex, berkeley DB and BIGSI in one image. See: https://bigsi.readme.io/docs for install instructions. 

## Quickstart

## Prepare the data

Requires [mccortex](github.com/mcveanlab/mccortex). 

	mccortex/bin/mccortex31 build -k 31 -s test1 -1 example-data/kmers.txt example-data/test1.ctx
	mccortex/bin/mccortex31 build -k 31 -s test2 -1 example-data/kmers.txt example-data/test2.ctx

#### Construct the bloom filters

	bigsi init test-bigsi --k 31 --m 1000 --h 1

	bigsi bloom --db test-bigsi -c example-data/test1.ctx example-data/test1.bloom
	bigsi bloom --db test-bigsi -c example-data/test2.ctx example-data/test2.bloom
	
### Build the combined graph

	bigsi build test-bigsi example-data/test1.bloom example-data/test2.bloom -s s1 -s s2

### Query the graph
	bigsi search -o tsv --db test-bigsi -s CGGCGAGGAAGCGTTAAATCTCTTTCTGACG

## Insert a new sample into the graph 

	bigsi insert test-bigsi example-data/test3.bloom s3


	

# Quickstart with docker

	docker pull phelimb/bigsi
	docker run phelimb/bigsi bigsi --help
	
### Preparing your data

BIGSI using single colour graphs to construct the coloured graph. 
Use [mccortex](https://github.com/mcveanlab/mccortex) to build. 
	
	PWD=`pwd`
	docker run -v $PWD/example-data:/data phelimb/bigsi mccortex/bin/mccortex31 build -k 31 -s test1 -1 /data/kmers.txt /data/test1.ctx
	docker run -v $PWD/example-data:/data phelimb/bigsi mccortex/bin/mccortex31 build -k 31 -s test2 -1 /data/kmers.txt /data/test2.ctx

### Building a BIGSI

#### Construct the bloom filters

	docker run -v $PWD/example-data:/data phelimb/bigsi bigsi  init /data/test.bigsi --k 31 --m 1000 --h 1

	docker run -v $PWD/example-data:/data phelimb/bigsi bigsi bloom --db /data/test.bigsi -c /data/test1.ctx /data/test1.bloom	
	docker run -v $PWD/example-data:/data phelimb/bigsi bigsi bloom --db /data/test.bigsi -c /data/test1.ctx /data/test2.bloom	
#### Build the combined graph
	docker run -v $PWD/example-data:/data phelimb/bigsi bigsi build /data/test.bigsi /data/test1.bloom /data/test2.bloom

#### Query the graph
	docker run -v $PWD/example-data:/data phelimb/bigsi bigsi search --db /data/test.bigsi -s CGGCGAGGAAGCGTTAAATCTCTTTCTGACG
	

## Citation

Please cite

```Real-time search of all bacterial and viral genomic data
Phelim Bradley, Henk den Bakker, Eduardo Rocha, Gil McVean, Zamin Iqbal
bioRxiv 234955; doi: https://doi.org/10.1101/234955 
```
if you use BIGSI in your work.