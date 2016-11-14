#! /usr/bin/env python
import begin


@begin.start
def main(N):
    N = int(N)
    ports = [
        "docker exec atlasseq_redismanager_1 /bin/bash -c 'yes yes | ./redis-trib.rb create --replicas 0"]
    for i in range(N):
        port = 7000+i+1
        port_string = "127.0.0.1:%i" % port
        ports.append(port_string)
    print(" ".join(ports)+"'")
