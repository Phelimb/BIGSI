# atlas-seq
[![Build Status](https://travis-ci.org/Phelimb/atlas-seq.svg)](https://travis-ci.org/Phelimb/atlas-seq)

	git clone https://github.com/Phelimb/atlas-seq.git

# Launch

First, clone the repository. 

	git clone --recursive https://github.com/Phelimb/atlas-seq.git
	
## With docker

Docker installation -  reccommended (install [docker toolbox](https://www.docker.com/products/docker-toolbox) first). 

	docker-compose pull && docker-compose up -d


## Without docker


	cd atlas-seq

	pip install -r requirements.txt
	python setup.py install

# Usage

Examples below are assuming you're running atlas-seq using docker-compose. To run without docker compose launch a redis instance `redis-server` and remove the references to `docker exec atlasseq_main_1` below. 

# Insert sample

sample.txt should be a text file of kmers. You can use tools like [mccortex](https://github.com/mcveanlab/mccortex), [cortex](https://github.com/iqbal-lab/cortex) or [jellyfish](https://github.com/gmarcais/Jellyfish) to quickly generate kmers from fastq/bam file. 

	docker exec atlasseq_main_1 atlasseq insert sample.txt

# Query for sequence

	docker exec atlasseq_main_1 atlasseq search -s CACCAAATGCAGCGCATGGCTGGCGTGAAAA
	docker exec atlasseq_main_1 atlasseq search -f seq.fasta

# Parameter choices:


## How do I decide on bloom filter size and number of hashes when building an atlas? 

### Short answer:

Use an onli\ne calculator to determine the bloom filter size (m) and number hashes (k) to give a false positive rate of ~0.05 for the number of kmers your expect per sample. 

e.g. for a Ecoli sequence set I'd expect ~5million unique kmers so I'd set m=30,000,00 and h = 4. (see [http://hur.st/bloomfilter?n=5000000&p=0.05](http://hur.st/bloomfilter?n=5000000&p=0.05))

The resulting disk/memory size of the atlas will be:

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



