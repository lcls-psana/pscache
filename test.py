
import cPickle
import numpy as np
import redis

from pscache import client
from pscache import publisher

import socket
if socket.gethostname()[:5] == 'psana':
    print 'psana machine, using psdb3 as REDIS target'
    HOST = 'psdb3'
else:
    HOST = 'localhost'
PORT = 6379
DB   = 0


class TestExptClient(object):

    # put some fake data into a redis db
    # test all fxns in client

    def setup(self):

        self._redis = redis.StrictRedis(host=HOST, port=PORT, db=DB)

        self.shape = (1,)
        self.type  = 'i8'
        info = '%s-%s' % (str(self.shape), self.type)

        # create instances of the 3 types of keys
        self._redis.sadd('runs', 47)
        self._redis.hset('run47:keyinfo', 'data', info) 
        for i in range(100):
            self._redis.rpush('run47:data', cPickle.dumps(i))

        self.client = client.ExptClient('testexpt', host=HOST)
        
        return

    def test_runs(self):
        r = self.client.runs()
        assert type(r) == set
        assert '47' in r

    def test_keys(self):
        ks = self.client.keys(47)
        assert type(ks) == list
        print ks
        assert 'data' in ks

    def test_keyinfo(self):
        ki = self.client.keyinfo(47)
        print ki
        assert type(ki) == dict
        assert 'data' in ki.keys()
        assert ki['data'][0] == self.shape
        assert ki['data'][1] == self.type

    def test_fetch_data(self):
        d = self.client.fetch_data(47, keys=['data'], max_events=50, fmt='list')
        assert type(d) == dict
        assert 'data' in d.keys()
        assert np.all(d['data'] == np.arange(50))
        assert d['data'].dtype == np.dtype(self.type)

    def test_minus_one(self):
        d = self.client.fetch_data(-1, keys=['data'], max_events=50, fmt='list')
        assert type(d) == dict
        assert 'data' in d.keys()
        assert np.all(d['data'] == np.arange(50))
        assert d['data'].dtype == np.dtype(self.type)

    def test_delete(self):
        print self.client.runs()
        assert '47' in self.client.runs()
        self.client.delete_run(47)
        assert '47' not in self.client.runs()
        assert 'run47:data' not in self._redis.keys()
        assert 'run47:keyinfo' not in self._redis.keys()

    def test_flush(self):
        self.client.flushdb()
        assert len(self._redis.keys('*')) == 0

    def teardown(self):
        self._redis.flushdb()


class TestExptPublisher(object):
    
    def setup(self):
        self._redis = redis.StrictRedis(host=HOST, port=PORT, db=DB)
        self.pub = publisher.ExptPublisher('testexpt', host=HOST)
        return
        
        
    def test_send_data(self):
        
        run  = 47
        key  = 'data'
        data = np.arange(50)
        self.pub._send_data(run, key, data, event_limit=-1)
        
        ret_data = self._redis.lrange('run%d:data' % run, 0, -1)
        assert [cPickle.loads(x) for x in ret_data] == range(50)
        
        keyinfo = self._redis.hgetall('run%d:keyinfo' % run)
        assert keyinfo['data'] == '(1,)-<i8'
        
        return
        
        
    def test_send_smalldata_dict(self):
       
        smd_dict = {'data' : np.arange(50)}
        run = 47
        self.pub.send_dict(smd_dict, run)
        
        ret_data = self._redis.lrange('run%d:data' % run, 0, -1)
        assert [cPickle.loads(x) for x in ret_data] == range(50)
        
        keyinfo = self._redis.hgetall('run%d:keyinfo' % run)
        assert keyinfo['data'] == '(1,)-<i8'
        
        return
        
        
    def test_smalldata_monitor(self):
       
        run = 9999
        def run_lookup_fxn():
            return run

        mon = self.pub.smalldata_monitor(run_lookup_fxn)
        
        smd_dict = {'data' : np.arange(50)}
        mon(smd_dict)
        
        ret_data = self._redis.lrange('run%d:data' % run, 0, -1)
        r = [cPickle.loads(x) for x in ret_data]
        assert r == range(50)
        
        keyinfo = self._redis.hgetall('run%d:keyinfo' % run)
        assert keyinfo['data'] == '(1,)-<i8'
        
        return


    def test_delete_run(self):

        smd_dict = {'data' : np.arange(50)}
        run = 47
        self.pub.send_dict(smd_dict, run)

        assert '47' in self._redis.smembers('runs')
        self.pub.delete_run(47)
        assert '47' not in self._redis.smembers('runs')


    def test_flush(self):
        self.pub.flushdb()
        assert len(self._redis.keys('*')) == 0


    def teardown(self):
        self._redis.flushdb()


