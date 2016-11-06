#! /usr/bin/env python
from jinja2 import Environment, FileSystemLoader
env = Environment(loader=FileSystemLoader('.'))
redis_conf = []
for i in range(2):
    redis = {}
    redis['i'] = i+1
    redis['port'] = 6300+i+1
    redis['host'] = 'redis%i' % (i+1)
    redis_conf.append(redis)
# print(redis_conf)
template = env.get_template('docker-compose.template')
print(template.render(redis_conf=redis_conf))
