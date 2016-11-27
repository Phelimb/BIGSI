import hashlib
import struct 
import sys
COMPLEMENT = {'A': 'T', 'C': 'G', 'G': 'C', 'T': 'A'}
BITS={'A':'00','G':'01','C':'10','T':'11'}
BASES={'00':'A','01':'G','10':'C','11':'T'}
from pyseqfile import Reader

def bitwise_AND(bytes a, bytes b):
    return (int.from_bytes(a, byteorder='big') & int.from_bytes(b, byteorder='big')).to_bytes(len(a), byteorder='big')

def kmer_reader(f):
    reader = Reader(f)
    for i,line in enumerate(reader):
        # if i % 100000 == 0 and i >0:
        #     sys.stderr.write(str(i)+'\n')
        #     sys.stderr.flush() 
        #     break
        read = line.decode('utf-8')
        for k in seq_to_kmers(read):
            yield k


def unique_kmers(f):
    return __unique_kmers(f)

cdef set __unique_kmers(f):
    a=set()
    reader = Reader(f)
    for i,line in enumerate(reader):
        if i % 100000 == 0 and i >0:
            sys.stderr.write(str(i)+'\n')
            sys.stderr.flush() 
        read = line.decode('utf-8')
        a.update(_seq_to_kmer_set(read))
    return a

cdef set _seq_to_kmer_set(str seq, int kmer_size = 31):
    return {seq[i:i+kmer_size] for i in range(len(seq)-kmer_size+1)}

def chunks(list l, int n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def decode_kmer(bytes binary_kmer, int kmer_size):
    """
    Returns a string representation of the specified kmer.
    """
    # G and C are the wrong way around because we reverse the sequence.
    # This really is a nasty way to do this!
    assert kmer_size <= 31
    binary_kmer_int = struct.unpack('Q', binary_kmer)[0]

    b = "{0:064b}".format(binary_kmer_int)[::-1]
    ret = []
    for j in range(kmer_size):
        nuc = BASES[b[j * 2: (j + 1) * 2]]
        ret.append(nuc)
    ret = "".join(ret)

    return ret[::-1]

cdef long encode_kmer(str kmer):
    """
    Returns the encoded integer representation of the specified string kmer.
    """
    ret = 0
    codes = {"A": 0, "C": 1, "G": 2, "T": 3}
    for j, nuc in enumerate(reversed(kmer)):
        v = codes[nuc]
        ret |= v << (2 * j)
    return ret#struct.pack('Q', ret)

def make_hash(str s):
    return hashlib.sha256(s.encode("ascii", errors="ignore")).hexdigest()

def hash_key(bytes k, int i=4):
    return hashlib.sha256(k).hexdigest()[:i]


def reverse_comp(str s):
    return "".join([COMPLEMENT.get(base, base) for base in reversed(s)])

def convert_query_kmers(kmers):
    return [convert_query_kmer(k) for k in kmers]

def convert_query_kmer(str kmer):
    return canonical(kmer)

def canonical(str k):
    l = [k,reverse_comp(k)]
    l.sort()
    return l[0]

def min_lexo(str k):
    l = [k,reverse_comp(k)]
    l.sort()
    return l[0]
    
def seq_to_kmers(str seq, int kmer_size = 31):
    for i in range(len(seq)-kmer_size+1):
        try:
            yield seq[i:i+kmer_size]
        except KeyError:
            pass

cdef set seq_to_kmer_set(str seq, int kmer_size = 31):
    return {seq[i:i+kmer_size] for i in range(len(seq)-kmer_size+1)}


def seq_to_encoded_kmers(str seq, int kmer_size=31):
    try:
        kmer=encode_kmer(seq[:kmer_size])
    except KeyError:
        seq_to_encoded_kmers(seq[1:])
    else:
        for base in seq[kmer_size:]:
            kmer= kmer >> 2
            bkmer=struct.pack('Q', kmer)
            yield bkmer

        

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
        bitstring = "".join([bitstring, '0'*bitpadding])[::-1]
    list_of_bytes = [bitstring[i:i+8] for i in range(0, len(bitstring), 8)]
    _bytes = [int(byte, 2) for byte in list_of_bytes]
    return bytes(_bytes)