#! /usr/bin/env python
from jinja2 import Environment, FileSystemLoader
import begin
env = Environment(loader=FileSystemLoader('.'))
redis_conf = []


@begin.start
def main(N):
    N = int(N)
    for i in range(N):
        redis = {}
        redis['i'] = i+1
        redis['port'] = 6300+i+1
        redis['host'] = 'redis%i' % (i+1)
        redis_conf.append(redis)
    template = env.get_template('docker-compose.template')
    print(template.render(redis_conf=redis_conf))
