# atlas-seq
[![Build Status](https://travis-ci.com/Phelimb/atlas-seq.svg?token=zS56Z2pmznVQKhUTxqcq&branch=master)](https://travis-ci.com/Phelimb/atlas-seq)

	git clone https://github.com/Phelimb/atlas-seq.git



# Launch
	

Docker installation -  reccommended (install docker toolbox first). 

	docker-compose pull && docker-compose up -d

# Insert sample

	docker exec redismcdbg_main_1 remcdbg/main.py insert sample.txt

# Query for sequence

	docker exec redismcdbg_main_1 remcdbg/main.py query /data/gn-amr-genes.fasta



