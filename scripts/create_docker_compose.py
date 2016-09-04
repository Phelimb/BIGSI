from jinja2 import Environment, FileSystemLoader
env = Environment(loader=FileSystemLoader('.'))
redis_conf = []
for i in range(64):
    redis = {}
    redis['i'] = i
    redis['port'] = 6300+i
    redis['host'] = 'redis%i' % i
    redis_conf.append(redis)
# print(redis_conf)
template = env.get_template('docker-compose.template')
print(template.render(redis_conf=redis_conf))
