"""
Fasta, Fastq, kmers-text file reader.
"""

from pyfastx import Fasta, Fastq
from bigsi.utils.cortex import extract_kmers_from_ctx
from bigsi.utils.fncts import canonical, seq_to_kmers

def extract_kmers_from_fasta(fasta_file, kmer_size, seq_type):
    if seq_type == 'nucleotides':
        for name, seq in Fasta(fasta_file, build_index=False):
            for kmer in seq_to_kmers(seq, kmer_size):
                yield canonical(kmer)
    else:
        for name, seq in Fasta(fasta_file, build_index=False):
            for kmer in seq_to_kmers(seq, kmer_size):
                yield kmer
        

def extract_kmers_from_fastq(fastq_file, kmer_size, seq_type):
    if seq_type == 'nucleotides':
        for name,seq,quality in Fastq(fastq_file, build_index=False):
            for kmer in seq_to_kmers(seq, kmer_size):
                yield canonical(kmer)
    else:
        for name,seq,quality in Fastq(fastq_file, build_index=False):
            for kmer in seq_to_kmers(seq, kmer_size):
                yield kmer
                

def extract_kmers_from_kmers_txt(kmers_txt, seq_type):
    if seq_type == 'nucleotides':
        for kmer in open(kmers_txt):
            yield canonical(kmer.strip())
    else:
        for kmer in open(kmers_txt):
            yield kmer.strip()


def check_file_type(in_file, kmer_size, seq_type):
    line = ""
    with open(in_file, "rb") as in_f:
        line = in_f.readline()
    if line[:6] == b"CORTEX":
        return extract_kmers_from_ctx(in_file, kmer_size)
    elif line.startswith(b">"):
        return extract_kmers_from_fasta(in_file, kmer_size, seq_type)
    elif line.startswith(b"@"):
        return extract_kmers_from_fastq(in_file, kmer_size, seq_type)
    elif line.isalpha:
        return extract_kmers_from_kmers_txt(in_file, seq_type)
