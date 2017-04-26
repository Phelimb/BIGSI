from celery import Celery
from cbg.graph import ProbabilisticMultiColourDeBruijnGraph as Graph
import os
from pyseqfile import Reader
from cbg.utils import seq_to_kmers
import json
import logging
logger = logging.getLogger(__name__)
from cbg.utils import DEFAULT_LOGGING_LEVEL
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


@app.task
def run_insert(kmers, bloom_filter, sample_name=None):
    graph = Graph(storage=storage,
                  bloom_filter_size=bloom_filter_size,
                  num_hashes=num_hashes)
    if sample_name is None:
        sample_name = os.path.basename(bloom_filter).split('.')[0]
    logger.debug("Starting insert. ")
    graph.insert(bloom_filter, sample_name)
    graph.sync()
    return {"message": "success",
            "colour": graph.get_colour_from_sample(sample_name),
            }
