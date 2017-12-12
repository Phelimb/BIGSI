#! /usr/bin/env python
import begin


@begin.start
def main(N):
    N = int(N)

    for i in range(N):
        port = 7000+i
        base_cmd = "docker exec bigsi_redis%i_1 bash -c 'cat /data/mobius/iqbal/redis_cmds/*/%i.txt | redis-cli --pipe -p %i '&" % (
            i+1, port, port)
        print(base_cmd)
