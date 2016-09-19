#! /usr/bin/env python
from flask import Flask
from flask import jsonify
from flask import request
from flask import abort
from celery import Celery
import os

import sys
import os
import time
import redis
sys.path.append(
    os.path.realpath(
        os.path.join(
            os.path.dirname(__file__),
            "..")))

from remcdbg.utils import seq_to_kmers
from remcdbg.mcdbg import McDBG
app = Flask('app')

CONN_CONFIG = []
redis_envs = [env for env in os.environ if "REDIS" in env]
if len(redis_envs) == 0:
    CONN_CONFIG = [('localhost', 6379)]
else:
    for i in range(int(len(redis_envs)/2)):
        hostname = os.environ.get("REDIS_IP_%s" % str(i + 1))
        port = int(os.environ.get("REDIS_PORT_%s" % str(i + 1)))
        CONN_CONFIG.append((hostname, port))


def load_mc(conn_config):
    try:
        return McDBG(conn_config=conn_config, storage={'redis': conn_config})
    except redis.exceptions.BusyLoadingError as e:
        time.sleep(10)
        print("%s" % str(e))
        return load_mc(conn_config)


def make_celery(app):
    celery = Celery(app.import_name, backend=app.config['CELERY_RESULT_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL']
                    )
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

if os.uname()[0] == "Darwin":
    _broker = 'localhost'  # debug
else:
    _broker = 'redisbroker'
app.config.update(
    CELERY_BROKER_URL='redis://%s:6379' % _broker,
    CELERY_RESULT_BACKEND='redis://%s:6379' % _broker  # ,
    # CELERY_ACCEPT_CONTENT=['json', 'msgpack', 'yaml']
)
celery = make_celery(app)


@app.route('/api/v1.0/search', methods=['POST'])
def search():
    mc = load_mc(conn_config=CONN_CONFIG)
    if not request.json or not 'seq' in request.json:
        abort(400)
    # http://stackoverflow.com/questions/26686850/add-n-tasks-to-celery-queue-and-wait-for-the-results
    tasks = {}
    for gene, seq in request.json['seq'].items():
        tasks[gene] = search_async(str(seq))
    # found = {}

    # for gene, task in tasks.items():
    #     found[gene] = task.get()
    return jsonify(tasks)


@celery.task
def search_async(seq):
    mc = load_mc(conn_config=CONN_CONFIG)

    start = time.time()
    kmers = [k for k in seq_to_kmers(seq)]
    results = mc.query_kmers(kmers, threshold=1)
    diff = time.time() - start
    return {'time': "%ims" % int(1000*diff), 'results': results}


@app.route('/api/v1.0/stats', methods=['get'])
def stats():
    mc = load_mc(conn_config=CONN_CONFIG)
    stats = {}
    stats["memory (bytes)"] = mc.calculate_memory()
    stats["count_kmers"] = mc.count_kmers()
    stats["samples"] = mc.get_num_colours()
    return jsonify(stats)

if __name__ == '__main__':
    app.run(debug=True)
