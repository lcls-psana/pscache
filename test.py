
import cPickle
import numpy as np
import redis

from pscache import client

class TestExptClient(object):

    # put some fake data into a local redis db
    # test all fxns in client

    def setup(self):

        self._redis = redis.StrictRedis(host='localhost', port=6379, db=0)


        self.shape = (1,)
        self.type  = 'i8'
        info = '%s-%s' % (str(self.shape), self.type)

        # create instances of the 3 types of keys
        self._redis.lpush('runs', 47)
        self._redis.hset('run47:keys', 'data', info) 
        for i in range(100):
            self._redis.rpush('run47:data', cPickle.dumps(i))

        self.client = client.ExptClient('testexpt', host='localhost')
        
        return


    def test_runs(self):
        r = self.client.runs()
        assert type(r) == list
        assert '47' in r

    def test_keys(self):
        ks = self.client.keys(47)
        assert type(ks) == list
        assert 'data' in ks

    def test_keyinfo(self):
        ki = self.client.keyinfo(47)
        assert type(ki) == dict
        assert 'data' in ki.keys()
        assert ki['data'][0] == self.shape
        assert ki['data'][1] == self.type

    def test_fetch_data(self):
        d = self.client.fetch_data(47, keys=['data'], max_events=50, fmt='list')
        assert len(d) == 1
        assert np.all(d[0] == np.arange(50))
        assert d[0].dtype == np.dtype(self.type)

    def teardown(self):
        self._redis.delete('runs', 'run47:keyinfo', 'run47:data')

