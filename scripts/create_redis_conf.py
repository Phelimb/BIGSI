#! /usr/bin/env python

# port 7000
# cluster-enabled yes
# cluster-config-file nodes.conf
# cluster-node-timeout 5000
# appendonly yes
import sys
import begin


@begin.start
def run(i):
    i = int(i)
    sys.stdout.write("port %i\n" % (7000 + (i-1)))
    sys.stdout.write("cluster-enabled yes\n")
    sys.stdout.write("cluster-config-file nodes.conf\n")
    sys.stdout.write("cluster-node-timeout 5000\n")
    sys.stdout.write("appendonly yes\n")
