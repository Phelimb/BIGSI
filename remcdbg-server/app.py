#! /usr/bin/env python
from flask import Flask
from flask import jsonify
from flask import request
from flask import abort
import sys
import os
import time
sys.path.append(
    os.path.realpath(
        os.path.join(
            os.path.dirname(__file__),
            "..")))

from remcdbg.utils import seq_to_kmers
from remcdbg.mcdbg import McDBG
app = Flask(__name__)

redis_envs = [env for env in os.environ if "REDIS" in env]
ports = sorted([int(os.environ.get(r)) for r in redis_envs])
mc = McDBG(ports=ports)


@app.route('/')
def index():
    return "Hello, World!"


@app.route('/api/v1.0/search', methods=['POST'])
def search():
    if not request.json or not 'seq' in request.json:
        abort(400)
    colours_to_samples = mc.colours_to_sample_dict()
    found = {}
    for gene, seq in request.json['seq'].items():
        found[gene] = {}
        found[gene]['samples'] = []
        start = time.time()
        kmers = [k for k in seq_to_kmers(str(seq))]
        _found = mc.query_kmers_100_per(kmers)
        for i, p in enumerate(_found):
            if p == 1:
                found[gene]['samples'].append(
                    colours_to_samples.get(i, 'missing'))
        diff = time.time() - start
        found[gene]['time'] = diff
    return jsonify(found)


@app.route('/api/v1.0/stats', methods=['get'])
def stats():
    stats = {}
    stats["memory (bytes)"] = mc.calculate_memory()
    stats["count_kmers"] = mc.count_kmers()
    stats["samples"] = mc.get_num_colours()
    return jsonify(stats)

if __name__ == '__main__':
    app.run(debug=True)
