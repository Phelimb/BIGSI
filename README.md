# BItsliced Genomic Signature Index [BIGSI]
<!--[![Build Status](https://travis-ci.org/Phelimb/bigsi.svg)](https://travis-ci.org/Phelimb/bigsi)-->

BIGSI can search a collection of raw (fastq/bam), contigs or assembly for genes, variant alleles and arbitrary sequence. It can scale to millions of bacterial genomes requiring ~3MB of disk per sample while maintaining millisecond kmer queries in the collection.

This tool was formerly named "Coloured Bloom Graph" or "CBG" in reference to the fact that it can be viewed as a coloured probabilistic de Bruijn graph.

Documentation can be found at https://bigsi.readme.io/. 
An index of the microbial ENA/SRA (Dec 2016) can be queried at http://www.bigsi.io. 

You can read more in our preprint here: https://www.biorxiv.org/content/early/2017/12/15/234955.

# Install

bigsi has a docker image that bundles mccortex, berkeley DB and BIGSI in one image. See: https://bigsi.readme.io/docs for install instructions. 

## Quickstart

Quickstart available at [https://bigsi.readme.io/docs/your-first-bigsi](https://bigsi.readme.io/docs/your-first-bigsi)
	

## Citation

Please cite

```Real-time search of all bacterial and viral genomic data
Phelim Bradley, Henk den Bakker, Eduardo Rocha, Gil McVean, Zamin Iqbal
bioRxiv 234955; doi: https://doi.org/10.1101/234955 
```
if you use BIGSI in your work.
