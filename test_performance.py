
from pscache import publisher
from pscache import client

import time
import numpy as np
import redis

import socket
if socket.gethostname()[:5] == 'psana':
    print 'psana machine, using psdb3 as REDIS target'
    HOST = 'psdb3'
else:
    HOST = 'localhost'
PORT = 6379
DB   = 0


# setup
pub = publisher.ExptPublisher('testexpt', host=HOST)
cli = client.ExptClient('testexpt', host=HOST)

run = 1
key = 'testdata'
cli.delete_run(run)

data_sizes = [(100000,), (100,100,100), (1,100000)]


print "------ TESTING REDIS PERFORMANCE ------"
print "HOST: %s" % HOST
print ""

print ">>> testing publisher"
for i,ds in enumerate(data_sizes):
    data = np.random.randn(*ds)
    t0 = time.time()
    pub._send_data(run, key+str(i), data, event_limit=-1)
    t1 = time.time()
    print "    %s ::\t%.3f sec" % (str(ds), t1-t0)

print ">>> testing client"
for i,ds in enumerate(data_sizes):
    data = np.random.randn(*ds)
    t0 = time.time()
    data = cli.fetch_data(run, keys=[key+str(i),])
    t1 = time.time()
    print "    %s ::\t%.3f sec" % (str(ds), t1-t0)
    assert data[key+str(i)].shape == ds

cli.delete_run(run)


