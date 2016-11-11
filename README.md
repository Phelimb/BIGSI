# atlas-seq
[![Build Status](https://travis-ci.com/Phelimb/atlas-seq.svg?token=zS56Z2pmznVQKhUTxqcq&branch=master)](https://travis-ci.com/Phelimb/atlas-seq)

	git clone https://github.com/Phelimb/atlas-seq.git



# Launch
	
## With docker

Docker installation -  reccommended (install [docker toolbox](https://www.docker.com/products/docker-toolbox) first). 

	docker-compose pull && docker-compose up -d


## Without docker

	git clone --recursive https://github.com/Phelimb/atlas-seq.git

	cd atlas-seq

	pip install -r requirements.txt
	python setup.py install

# Usage

Examples below are assuming you're running atlas-seq using docker-compose. To run without docker compose launch a redis instance `redis-server` and remove the references to `docker exec atlasseq_main_1` below. 

# Insert sample

sample.txt should be a text file of kmers. You can use tools like [mccortex](https://github.com/mcveanlab/mccortex), [cortex](https://github.com/iqbal-lab/cortex) or [jellyfish](https://github.com/gmarcais/Jellyfish) to quickly generate kmers from fastq/bam file. 

	docker exec atlasseq_main_1 atlasseq insert sample.txt

# Query for sequence

	docker exec atlasseq_main_1 atlasseq search -s CACCAAATGCAGCGCATGGCTGGCGTGAAAA	docker exec atlasseq_main_1 atlasseq search -f seq.fasta