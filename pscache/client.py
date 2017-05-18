
import redis
import cPickle
import threading
import time
import numpy as np



class TopicClient(threading.Thread):
    
    def __init__(self, topic):
        
        threading.Thread.__init__(self)
        
        self._redis = redis.StrictRedis(host='localhost',
                                        port=6379, 
                                        db=0, 
                                        password=None, 
                                        socket_timeout=None, 
                                        socket_connect_timeout=None, 
                                        socket_keepalive=None, 
                                        socket_keepalive_options=None, 
                                        connection_pool=None, 
                                        unix_socket_path=None, 
                                        retry_on_timeout=False,  
                                        max_connections=None)
        
        self._pubsub = self._redis.pubsub()
        self._pubsub.subscribe(topic)
        
        return
    
        
    def keys(self, pattern='*'):
        return self._redis.keys(pattern)
        
        
    def query(self, retry_time=0.01):
        
        item = self._pubsub.get_message()
        if item is not None:
            if item['type'] == 'message':
                d = cPickle.loads(item['data'])
                return d

        # messages are also sent to indicate pub/subs etc
        # this recursion ensures we get a message
        time.sleep(retry_time)
        self.query()
        
        return 
    
    # ----------------------
    
    def follow(self):
        self.start()
        # calls through to run
        return
        
        
    def run(self):
        """
        Note that run is a special method used by threading. It is
        employed by a spawned thread when self.start() is called.
        """
        
        for item in self._pubsub.listen():
            if item['data'] == "KILL":
                self._pubsub.unsubscribe()
                print self, "unsubscribed and finished"
                break
            else:
                print item['channel'], ":", item['data']
                
        return
        
        
class ExptClient(object):
    
    def __init__(self, experiment, host='localhost', **kwargs):
        
        # todo : set db based on experiment ?
        # 
        
        self._redis = redis.StrictRedis(host=host,
                                        port=6379, 
                                        db=0, 
                                        **kwargs)
                                        
        return
        
        
    def runs(self):
        """
        Get a list of runs in the Redis database for this experiment.
        
        Returns
        -------
        runs : list of ints
            List of runs.
        """
        return self._redis.smembers('runs')
        
        
    def _run_check(self, run):
        """
        Ensure run gets mapped to the right lookup value
        """
    
        run = int(run)
    
        if run < 0:
            runs = list(self.runs())
            runs.sort()
            run = int(runs[run])
            assert run > 0
        if run == 0:
            raise ValueError('run=0 is not a valid option')
    
        return run
        
        
    def keys(self, run):
        """
        Get a list of keys associated with the specific run.
        
        Parameters
        ----------
        run : int
            The run number. Use -1 to get the most recent run (-2 to get 2nd
            most recent, etc).
        
        Returns
        -------
        keyinfo : dict
            A dictionary mapping key names --> (shape, type)
        """
        name = 'run%d:keyinfo' % self._run_check(run)
        return self._redis.hkeys(name)
        
        
    def keyinfo(self, run):
        """
        Get a list of keys associated with the specific run, along with the
        shapes and types of the data they point to.
        
        Parameters
        ----------
        run : int
            The run number. Use -1 to get the most recent run (-2 to get 2nd
            most recent, etc).
        
        Returns
        -------
        keyinfo : dict
            A dictionary mapping key names --> (shape, type)
        """
        
        run = self._run_check(run)
        
        tuple_strip = lambda s : s.strip('(').strip(')').strip()
        
        d = self._redis.hgetall('run%s:keyinfo' % run)
        for k in d.keys():
            shp, typ = d[k].split('-')
            
            shp = [ tuple_strip(x) for x in shp.split(',') ]
            if '' in shp:
                shp.remove('')
            shp = tuple([int(x) for x in shp])
            
            d[k] = (shp, typ)
        
        return d
       
        
    def fetch_data(self, run, keys=[], max_events=-1, fmt='list'):
        """
        Get the data for a set of keys (by default all) for a specific run.
        
        Parameters
        ----------
        run : int
            The run number. Use -1 to get the most recent run (-2 to get 2nd
            most recent, etc).
        
        keys : list of str
            A list of strings, each string being a key to fetch. Pass an empty
            list to get all keys for the run.
        
        max_events : int
            The maximum number of events to fetch. -1 (default) means fetch all.
        
        fmt : str {'list', 'xarray'}
            What format to return the data in.
        
        Returns
        -------
        data : dict
            The data requested, in a dict where key --> np.array.
        """
        
        run = self._run_check(run)
        
        if len(keys) == 0:
            keys = self.keys(run)
            
        # fetch data into a list
        data = {}
        for k in keys:
            name = 'run%d:%s' % (run, k)
            d = [cPickle.loads(s) for s in self._redis.lrange(name, 0, max_events-1)]
            data[k] = np.array(d)
            
        # reformat if requested
        if fmt == 'list':
            pass
        elif fmt == 'xarray':
            raise NotImplementedError() # todo
        else:
            raise ValueError('`fmt` must be one of {"list", "xarray"}')
            

        return data


    def delete_run(self, run):
        self._redis.delete(self.keys(run))
        self._redis.srem('runs', run)
        return


    def flushdb(self):
        self._redis.flushdb()
        return

                
if __name__ == '__main__':
    
    c = TopicClient('number')
    
    time.sleep(1)
    
    print '---- single queries ----'
    c.query()
    c.query()
    c.query()
    
    print '----   following    ----'
    c.follow()
    
