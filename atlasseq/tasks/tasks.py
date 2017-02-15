from celery import Celery
from atlasseq.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import os
from pyseqfile import Reader
from atlasseq.utils import seq_to_kmers
import json
import logging
logger = logging.getLogger(__name__)
from atlasseq.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)
hostname = os.environ.get("BROKER_IP", "localhost")

app = Celery('tasks', backend='redis://%s:6400' % hostname,
             broker='redis://%s:6400' % hostname)


def kmer_reader(f):
    reader = Reader(f)
    for i, line in enumerate(reader):
        # if i % 100000 == 0:
        #     sys.stderr.write(str(i)+'\n')
        #     sys.stderr.flush()
        read = line.decode('utf-8')
        for k in seq_to_kmers(read):
            yield k


def insert_kmers(mc, kmers, colour, sample, count_only=False):
    if not count_only:
        graph.insert_kmers(kmers, colour)
    graph.add_to_kmers_count(kmers, sample)


def insert_from_kmers(kmers, kmer_file, graph,
                      force=False, sample_name=None, intersect_kmers_file=None, sketch_only=False):

    if sample_name is None:
        sample_name = os.path.basename(kmer_file).split('.')[0]

    if intersect_kmers_file is not None:
        intersect_kmers = set(load_all_kmers(intersect_kmers_file))
    else:
        intersect_kmers = None

    if kmer_file is not None:
        kmers = {}.fromkeys(kmer_reader(kmer_file)).keys()

    logger.debug("Starting insert. ")
    try:
        graph.insert(kmers, sample_name, sketch_only=sketch_only)
        graph.sync()
        return {"message": "success",
                "colour": graph.get_colour_from_sample(sample_name),
                "total-kmers": graph.count_kmers(),
                "kmers-added": graph.count_kmers(sample_name),
                #                          "memory": graph.calculate_memory()
                }
    except ValueError as e:
        if not force:
            return {"result": "failed", "message": str(e),
                    "total-kmers": graph.count_kmers(),
                    "kmers-added": graph.count_kmers(sample_name),
                    # "memory": graph.calculate_memory()
                    }
        else:
            raise NotImplemented("Force not implemented yet")


def insert_from_merge_results(merge_results, graph, force=False):
    with open(merge_results, 'r') as infile:
        metadata = json.load(infile)
    # insert samples
    if "0" in metadata.get('graph'):
        for i, s in enumerate(metadata.get('cols')):
            try:
                graph._add_sample(s)
            except ValueError as e:
                if force:
                    graph._add_sample(s+str(i))
                else:
                    logger.warning(e)
    indexes = [int(i) for i in metadata['graph'].keys()]
    logger.info("Inserting rows %i to %i" % (min(indexes), max(indexes)))
    for row, bitarray_f in metadata.get('graph').items():
    for row in sorted(indexes):
        row = str(row)
        bitarray_f = metadata['graph'][row]
        logger.info("Inserting row %s" % row)
        with open(bitarray_f, 'rb') as inf:
            res = inf.read()
            graph.graph[row] = res
    graph.sync()
    return {"result": "success"}


@app.task
def run_insert(kmers, kmer_file, merge_results, storage, bloom_filter_size, num_hashes,
               force=False, sample_name=None, intersect_kmers_file=None, sketch_only=False):
    graph = Graph(storage=storage,
                  bloom_filter_size=bloom_filter_size,
                  num_hashes=num_hashes)
    if not merge_results:
        return insert_from_kmers(kmers, kmer_file, graph, force=force, sample_name=sample_name,
                                 intersect_kmers_file=intersect_kmers_file, sketch_only=sketch_only)
    else:
        return insert_from_merge_results(merge_results, graph, force=force)
