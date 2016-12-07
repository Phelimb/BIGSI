"""
Cortex file reader.
## Adapted with thanks from code writen by https://github.com/jeromekelleher/
"""
from __future__ import print_function
from __future__ import division

__version__ = "0.0.1"


import os
import json
import gzip
import struct
import subprocess
import math
BITS = {'A': '00', 'G': '01', 'C': '10', 'T': '11'}
BASES = {'00': 'A', '01': 'G', '10': 'C', '11': 'T'}


def kmer_to_bits(kmer):
    return "".join([BITS[k] for k in kmer])


def decode_kmer(binary_kmer, kmer_size):
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


def decode_edges(edges):
    """
    Decodes the specified integer representing edges in Cortex graph. Returns
    a tuple (forward, reverse) which contain the list of nucleotides that we
    append to a kmer and its reverse complement to obtain other kmers in the
    Cortex graph.
    """
    bases = ["A", "C", "G", "T"]
    fwd = []
    for j in range(4):
        if (1 << j) & edges != 0:
            fwd.append(bases[j])
    rev = []
    bases.reverse()
    for j in range(4):
        if (1 << (j + 4)) & edges != 0:
            rev.append(bases[j])
    return fwd, rev


def encode_kmer(kmer):
    """
    Returns the encoded integer representation of the specified string kmer.
    """
    ret = 0
    codes = {"A": 0, "C": 1, "G": 2, "T": 3}
    for j, nuc in enumerate(reversed(kmer)):
        v = codes[nuc]
        ret |= v << (2 * j)
    return struct.pack('Q', ret)


def reverse_complement(kmer):
    """
    Returns the reverse complement of the specified string kmer.
    """
    d = {"A": "T", "C": "G", "G": "C", "T": "A"}
    # This is very slow and nasty!
    s = ""
    for c in kmer:
        s += d[c]
    return s[::-1]


def canonical_kmer(kmer):
    """
    Returns the canonical version of this kmer, which is the lexically
    least of itself and its reverse complement.
    """
    rev = reverse_complement(kmer)
    return rev if rev < kmer else kmer


class Kmer(object):

    """
    A class representing fixed length strings of DNA nucleotides. A Kmer
    is equal to its reverse complement, and the canonical representation
    of a give kmer is the lexically least of itself  and its reverse
    complement.
    """

    def __init__(self, kmer):
        self.value = kmer
        self.canonical_value = canonical_kmer(kmer)

    def __str__(self):
        return self.canonical_value


class CortexRecord(object):

    """
    Class representing a single record in a cortex graph. A record
    consists of a kmer, its edges and coverages in the colours.
    """

    def __init__(self, kmer_size, kmer, coverages, edges, num_colours=1, binary_kmer=False):
        if binary_kmer:
            self.kmer = kmer
        else:
            self.kmer = Kmer(decode_kmer(kmer, kmer_size))
        self.coverages = coverages
        self.edges = [decode_edges(e) for e in edges]
        self.num_colours = num_colours

    def __str__(self):
        return "<CortexRecord %s - %i colour(s)>" % (self.kmer, self.num_colours)

    def print(self, colour):
        nucleotides = "ACGT"
        s = ["." for j in range(8)]
        for j, n in enumerate(nucleotides):
            if n in self.edges[colour][1]:
                s[j] = n.lower()
        for j, n in enumerate(nucleotides):
            if n in self.edges[colour][0]:
                s[j + 4] = n
        s = "".join(s)
        return ("{0} {1} {2}".format(self.kmer, self.coverages[colour], s))

    def get_adjacent_kmers(self, colour=0, direction=0):
        """
        Returns the kmers adjacent to this kmer using the edges in the
        record.
        """
        fwd, rev = self.edges[colour]
        if direction == 0:
            for n in fwd:
                yield Kmer(self.kmer.canonical_value[1:] + n)
        else:
            for n in rev:
                yield Kmer(n + self.kmer.canonical_value[:-1])


class GraphReader(object):

    """
    Class to read a cortex graph.
    """

    def __init__(self, graph_file, binary_kmers=False):
        self._file_name = graph_file
        self._file = open(graph_file, "rb")
        self.read_header()
        self.binary_kmers = binary_kmers  # report kmers as bytes

    def read_unsigned_int(self):
        """
        Reads an uint32_t from the stream.
        """
        b = self._file.read(4)
        return struct.unpack("<I", b)[0]

    def read_header(self):
        """
        Reads the header of the graph file.
        """
        magic_str = b"CORTEX"
        b = self._file.read(len(magic_str))
        if b != magic_str:
            raise ValueError("File format mismatch")
        self.version = self.read_unsigned_int()
        if self.version != 6:
            raise ValueError("File format version error; only 6 supported")
        self.kmer_size = self.read_unsigned_int()
        self.kmer_storage_size = 8 * self.read_unsigned_int()
        self.num_colours = self.read_unsigned_int()
        self.record_size = self.kmer_storage_size + 5 * self.num_colours
        # skip per colour mean_read_length and total_sequence
        skip = self.num_colours * 12
        self._file.seek(skip, os.SEEK_CUR)
        for j in range(self.num_colours):
            v = self.read_unsigned_int()
            self._file.seek(v, os.SEEK_CUR)
        # skip per colour error rates
        skip = self.num_colours * 16  # sizeof(long double)
        self._file.seek(skip, os.SEEK_CUR)
        for j in range(self.num_colours):
            # skip cleaning counters
            self._file.seek(12, os.SEEK_CUR)
            v = self.read_unsigned_int()
            self._file.seek(v, os.SEEK_CUR)
        # header ends with the magic word
        b = self._file.read(len(magic_str))
        if b != magic_str:
            raise ValueError("File format mismatch")
        payload_start = self._file.tell()
        self._file.seek(0, os.SEEK_END)
        payload_size = self._file.tell() - payload_start
        self.num_records = payload_size // self.record_size
        self._file.seek(payload_start, os.SEEK_SET)

    def __iter__(self):
        return self

    def __next__(self):
        """
        Returns the next record
        """
        buf = self._file.read(self.record_size)
        if len(buf) == 0:
            raise StopIteration()
        return self.decode_record(buf)

    def next(self):
        # Python 2 compat - TODO what is the recommended way to do this?
        return self.__next__()

    def decode_record(self, buff):
        """
        Decodes the specified graph record.
        """

        kmer = buff[0:8]  #
        # print(buff[0:8], kmer)
        offset = 8
        coverages = struct.unpack_from("I" * self.num_colours, buff, offset)
        offset += self.num_colours * 4
        edges = struct.unpack_from("B" * self.num_colours, buff, offset)
        # print(edges)
        record = CortexRecord(
            self.kmer_size, kmer, coverages, edges, num_colours=self.num_colours, binary_kmer=self.binary_kmers)
        return record


class LinksRecord(object):

    """
    Class representing a single links record attached to a kmer.
    """

    def __init__(self, direction, num_kmers, counts, junctions):
        self.direction = direction
        self.num_kmers = num_kmers
        self.counts = counts
        self.junctions = junctions

    def __str__(self):
        return "{0}:{1}:{2}:{3}".format(self.direction, self.num_kmers, self.counts,
                                        self.junctions)


class LinksFile(object):

    """
    Class to read a cortex links file.
    """

    def __init__(self, filename):
        self._file = gzip.open(filename, "r")
        self._read_header()

    def _read_header(self):
        """
        Reads the header of the links file and a parses the relevant fields.
        """
        not_done = True
        open_braces = 0
        closed_braces = 0
        header = ""
        while not_done:
            s = self._file.readline()
            open_braces += s.count("{")
            closed_braces += s.count("}")
            not_done = open_braces != closed_braces
            header += s
        metadata = json.loads(header)
        if "fileFormat" in metadata:
            assert metadata["fileFormat"] == "ctp"
            assert metadata["formatVersion"] == 2
            self.num_kmers_with_paths = metadata["num_kmers_with_paths"]
            self.num_paths = metadata["num_paths"]
            self.ncols = metadata["ncols"]
            self.kmer_size = metadata["kmer_size"]
            self.num_kmers_in_graph = metadata["num_kmers_in_graph"]
            self.colours = metadata["colours"]
            self.commands = metadata["commands"]
        else:
            assert metadata["file_format"] == "ctp"
            assert metadata["format_version"] == 3
            graph = metadata["graph"]
            self.kmer_size = graph["kmer_size"]
            self.num_colours = graph["num_colours"]
            self.num_kmers_in_graph = graph["num_kmers_in_graph"]
            self.colours = graph["colours"]
            paths = metadata["paths"]
            self.num_kmers_with_paths = paths["num_kmers_with_paths"]

    def __iter__(self):
        return self

    def __next__(self):
        """
        Return the next record.
        """
        # Skip empty lines and comments
        s = self._file.readline()
        if s == "":
            raise StopIteration()
        s = s.lstrip()
        while len(s) == 0 or s.startswith("#"):
            s = self._file.readline()
            if s == "":
                raise StopIteration()
            s = s.lstrip()
        # Now read the header for the kmer.
        split = s.split()
        kmer = split[0]
        num_paths = int(split[1])
        paths = []
        for j in range(num_paths):
            split = self._file.readline().split()
            direction = split[0]
            num_kmers = int(split[1])
            num_juncs = int(split[2])
            counts = [int(x) for x in split[3].split(",")]
            juncs = split[4]
            assert num_juncs == len(juncs)
            lr = LinksRecord(direction, num_kmers, counts, juncs)
            paths.append(lr)
        return kmer, paths

    def next(self):
        """
        Python 2.x compatability.
        TODO check the recommended way to do this in 3.x and 2.x.
        """
        return self.__next__()


class GraphTraverser(object):

    """
    Class to traverse a graph using links information.
    """

    def __init__(self, graph_reader, links_file):
        self._graph = {}
        for r in graph_reader:
            self._graph[r.kmer.canonical_value] = r
        self._links = {}
        for kmer, link_records in links_file:
            assert len(kmer) == links_file.kmer_size
            self._links[kmer] = link_records
        assert links_file.num_kmers_with_paths == len(self._links)

    def traverse(self, seed):
        """
        Performs a simple traversal for the specified seed, returning the
        resulting contig.
        """
        # print("seed = ", seed)
        k = seed
        contig = seed[:-1]
        o = 0
        paths = []
        while k is not None:
            b = k[-1] if o == 0 else reverse_complement(k[0])
            contig += b
            revcmp = reverse_complement(k)
            c = k
            if revcmp < k:
                o = (o + 1) % 2
                c = revcmp
            direction = "F" if o == 0 else "R"
            if c in self._links:
                for lr in self._links[c]:
                    if lr.direction == direction:
                        paths.append([0, lr.junctions])
            adj = [
                obj.value for obj in self._graph[c].get_adjacent_kmers(0, o)]
            # print(k, c, o, adj, contig)
            # print("paths = ")
            # for p in paths:
            #     print("\t", p)
            k = None
            if len(adj) == 1:
                k = adj[0]
            elif len(adj) > 1:
                junctions = paths[0][1]
                # Choose the branch, if we can.
                junction = junctions[0]
                j = -1
                b = junction
                if o != 0:
                    j = 0
                    b = reverse_complement(junction)
                for kp in adj:
                    if kp[j] == b:
                        k = kp
                if k is None:
                    print("no junctions matched")
                # update the paths
                oldpaths = paths
                paths = []
                for age, junctions in oldpaths:
                    if junctions[0] == junction and len(junctions) > 1:
                        # print("updating", age, junctions)
                        paths.append([age + 1, junctions[1:]])

        return contig


def run_cortex(command, args, path="./ctx31"):
    """
    Runs the specified command on cortex, raising an error if the exit status
    is non zero.
    """
    cmd = [path, command, "-q", "-f", "-m", "100M", "-t", "10"] + args
    subprocess.check_call(cmd)


def build_graph(kmer_size, fasta_file, cortex_file, links_file=None):
    """
    Builds a graph from the specified fasta file and writes it to the
    specified cortex file. If the links_file is not None, also run
    thread to thread the sequences through the graph.
    """
    cmd = ["-s", "sample", "-k", str(kmer_size), "-1", fasta_file, cortex_file]
    run_cortex("build", cmd)
    if links_file is not None:
        cmd = ["-1", fasta_file, "-o", links_file, cortex_file]
        run_cortex("thread", cmd)
