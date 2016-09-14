import hashlib
COMPLEMENT = {'A': 'T', 'C': 'G', 'G': 'C', 'T': 'A'}
BITS={'A':'00','C':'01','G':'10','T':'11'}
BASES={'00':'A','01':'C','10':'G','11':'T'}

def make_hash(str s):
    return hashlib.sha256(s.encode("ascii", errors="ignore")).hexdigest()

def hash_key(bytes k):
    return hashlib.sha256(k).hexdigest()[:4]


def reverse_comp(str s):
    return "".join([COMPLEMENT.get(base, base) for base in reversed(s)])

def convert_query_kmers(kmers):
    return [convert_query_kmer(k) for k in kmers]

def convert_query_kmer(str kmer):
    return min_lexo(kmer)

def min_lexo(str k):
    k_hash=make_hash(k)
    _rev_comp = reverse_comp(k)
    _rev_comp_hash= make_hash(reverse_comp(k))
    d={k_hash:k,_rev_comp_hash:_rev_comp}
    l = [k_hash,_rev_comp_hash]
    l.sort()
    return d[l[0]]

def seq_to_kmers(str seq):
    for i in range(len(seq)-31+1):
        yield seq[i:i+31]

def bits(f):
    return [(s >> i) & 1 for s in f for i in xrange(7, -1, -1)]


def kmer_to_bits(str kmer):
    return "".join([BITS[k] for k in kmer])

def bits_to_kmer(str bitstring, int l):
    bases=[]
    for i in range(0,l*2,2):
         bases.append( BASES[bitstring[i:i+2]])
    return "".join(bases)


def kmer_to_bytes(str kmer,int bitpadding=0):
    bitstring = kmer_to_bits(kmer)
    if not bitpadding == 0:
        bitstring = "".join([bitstring, '0'*bitpadding])
    list_of_bytes = [bitstring[i:i+8] for i in range(0, len(bitstring), 8)]
    _bytes = [int(byte, 2) for byte in list_of_bytes]
    return bytes(_bytes)