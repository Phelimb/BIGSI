# atlas-seq
[![Build Status](https://travis-ci.com/Phelimb/atlas-seq.svg?token=zS56Z2pmznVQKhUTxqcq&branch=master)](https://travis-ci.com/Phelimb/atlas-seq)

	git clone https://github.com/Phelimb/atlas-seq.git



# Launch
	
# With docker

Docker installation -  reccommended (install [docker toolbox](https://www.docker.com/products/docker-toolbox) first). 

	docker-compose pull && docker-compose up -d

# Insert sample

	docker exec atlasseq_main_1 atlasseq insert sample.txt

# Query for sequence

	docker exec atlasseq_main_1 atlasseq query /data/gn-amr-genes.fasta


# Without docker

	pip install atlasseq



