
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
    
    def __init__(self, expt, host='localhost', **kwargs):
        
        self._redis = redis.StrictRedis(host=host, port=6379, db=0, 
                                        **kwargs)
                                        
        return
        

    def smalldata_monitor(self, keys=None, event_limit=-1):
        """
        Create a monitor function to hook the publisher into the
        psana SmallData interface.

        Parameters
        ----------
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

        run = 9999 # TODO how do we get this??

        monitor = partial(self.send_dict,
                          run=run,
                          keys=keys,
                          event_limit=event_limit)

        return monitor
        
    
    def send_dict(self, smd_dict, run, keys=None, event_limit=-1):
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
        
        self._redis.sadd('runs', run) # ensure run is there

        if keys == None:
            keys = smd_dict.keys()
            
        for k in keys:
            self._send_data(run, k, smd_dict[k], event_limit=event_limit)

        return
        
        
    def _send_data(self, run, key, data, event_limit=-1):
        """
        Parameters
        ----------
        run : int
            Run number
        
        key : str
            The data key
        
        data : ndarray
            First dimension is event index
        """
        
        # check keyinfo is correct
        shp = data[0].shape
        if shp == (): shp = (1,)
        info = '%s-%s' % (str(shp), data[0].dtype.str)
        
        if self._redis.hexists('run%d:keyinfo' % run, key):
            # TODO next line may not be necessary
            assert self._redis.hget('run%d:keyinfo' % run, key) == info 
        else:
            self._redis.hset('run%d:keyinfo' % run, key, info)
        
        # send data & trim excess
        pipe = self._redis.pipeline()
        
        for i in range(data.shape[0])[::-1]: # filo
            pipe.lpush('run%d:%s' % (run, key), 
                       cPickle.dumps(data[i]))
            
        pipe.ltrim('run%d:%s' % (run, key), 0, event_limit)
        pipe.execute()
        
        return
        
        
if __name__ == '__main__':
    
    import time
    
    p = Publisher('experiment_number')
    
    for i in range(30):
        p.send('number', i)
        time.sleep(0.1)

    p.send('number', 'KILL')
