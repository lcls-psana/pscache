
import redis
import cPickle
from functools import partial

class Publisher(object):
    
    def __init__(self, expt, host='localhost'):
        
        # TODO map experiment to DB or Redis instance
        
        self._redis = redis.StrictRedis(host=host, port=6379, db=0, 
                                        **kwargs)
                                        
        self._pubsub = self._redis.pubsub()
        return

        
    def send(self, topic, message):
        self._redis.publish(topic, cPickle.dumps(message))
        return


class ExptPublisher(object):
    
    def __init__(self, expt, host='localhost', verbose=False, 
                 auto_expire=1800, **kwargs):

        self.verbose = verbose
        self.auto_expire = auto_expire
        self._redis = redis.StrictRedis(host=host, port=6379, db=0, 
                                        **kwargs)
                                        
        return
        

    def smalldata_monitor(self, run_lookup_fxn, keys=None, event_limit=None):
        """
        Create a monitor function to hook the publisher into the
        psana SmallData interface.

        Parameters
        ----------
        run_lookup_fxn : function
            A function that returns the current run number

        keys : list of str
            A list of the keys to send to the pubisher. `None`
            (default) indicates all should be sent.

        event_limit : int
            The maximum number events to keep on the redis server
            at any one time.

        Returns
        -------
        monitor_function : callable
            Returns a callable that can be passed to 
            psana.SmallData.add_monitor_function

        Example
        -------
        >>> publisher = Publisher('xpptut15')
        >>> mt_fxn = publisher.smalldata_monitor(event_limit=1200)
        >>> smd.add_monitor_function(mt_fxn)
        """

        self._run_lookup_fxn = run_lookup_fxn

        monitor = partial(self.send_dict,
                          run=None,
                          keys=keys,
                          event_limit=event_limit)

        return monitor
        
    
    def send_dict(self, smd_dict, run, keys=None, event_limit=None):
        """
        Publish dictionary `smd_dict` to REDIS under `run`.
        
        Parameters
        ----------
        smd_dict : dict
            Keys are data names, values are np.ndarray. Arrays (values) should
            all be the same size in the first dimension (which indexes event #).
        
        run : int
            The run number to post to
        
        keys : list
            A list of the keys to send (subset of what is in `smd_dict`)
        
        event_limit : int
            The maximum number of events to send. Negative indexing is OK, so
            e.g. "-1" means send all.
        """

        if run == None:
            if hasattr(self, '_run_lookup_fxn'):
                run = self._run_lookup_fxn() # lookup latest!!
            else:
                raise ValueError('run=None not acceptable')
        
        if self.verbose: print 'sending data...'

        if keys == None:
            keys = smd_dict.keys()
            
        pipe = self._redis.pipeline()
        for k in keys:
            if self.verbose: print 'sending run%d:%s [%d evts]' % (run, k, smd_dict[k].shape[0])
            self._send_data(run, k, smd_dict[k], event_limit=event_limit,
                            external_pipeline=pipe)
        pipe.execute()

        return
        
        
    def _send_data(self, run, key, data, event_limit=None,
                   external_pipeline=None):
        """
        Parameters
        ----------
        run : int
            Run number
        
        key : str
            The data key
        
        data : ndarray
            First dimension is event index
                   
        external_pipeline : redis.Pipeline
            If provided, add to this pipeline. Otherwise create a new pipeline.
            If an external_pipeline is provided, the pipeline will not be
            executed by this function.
        """

        # sometimes we might get called with no data...
        # if so, just return
        if data.shape == (0,):
            return

        # check keyinfo is correct
        shp = data[0].shape
        if shp == (): shp = (1,)
        info = '%s-%s' % (str(shp), data[0].dtype.str)
        
        # if self._redis.hexists('run%d:keyinfo' % run, key):
        #     # TODO next line may not be necessary
        #     assert self._redis.hget('run%d:keyinfo' % run, key) == info
        # else:
        self._redis.hset('run%d:keyinfo' % run, key, info)
        
        # send data & trim excess
        if external_pipeline is None:
            pipe = self._redis.pipeline()
        else:
            pipe = external_pipeline

        for i in range(data.shape[0])[::-1]: # filo
            pipe.lpush('run%d:%s' % (run, key), 
                       cPickle.dumps(data[i]))
            
        if event_limit is not None:
            pipe.ltrim('run%d:%s' % (run, key), 0, event_limit)
            
        if self.auto_expire is not None:
            if self.auto_expire > 0:
                if self.verbose:
                    print('run %d key %s :: autoexpires in %d' % (run, key, self.auto_expire))
                pipe.expire('run%d:%s' % (run, key), self.auto_expire)
        
        if external_pipeline is None:
            pipe.execute()
        
        return
        
        
    def set_run_to_expire(self, run, time):
        """
        time : time in seconds
        """
        name = 'run%d:keyinfo' % run
        keys = self._redis.hkeys(name)
        
        self._redis.expire(name, time)
        for key in keys:
            self._redis.expire(key, time)
            
        return


    def delete_run(self, run):
        name = 'run%d:keyinfo' % run
        keys = self._redis.hkeys(name)
        self._redis.delete(keys)
        self._redis.delete(name)
        return


    def flushdb(self):
        self._redis.flushdb()
        return        

        
if __name__ == '__main__':
    
    import time
    
    p = Publisher('experiment_number')
    
    for i in range(30):
        p.send('number', i)
        time.sleep(0.1)

    p.send('number', 'KILL')
