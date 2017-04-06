
import redis
import cPickle

# pipe = r.pipeline()

class Publisher(object):
    
    def __init__(self, exp):
        
        # TODO map experiment to DB or Redis instance
        
        self._redis = redis.StrictRedis(host='psdb3', port=6379, 
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
        return

        
    def send(self, topic, message):
        self._redis.publish(topic, cPickle.dumps(message))
        return


    def _send_smalldata_dict(self, smd_dict, keys=None, shot_limit=None):


        if keys is not None:
            send_dict = {}
            for k in keys:
                send_dict[k] = smd_dict[k] # makes copies... (todo: can they be references)?
        else:
            send_dict = smd_dict           # all keys references, not copies


        if shot_limit is not None:
            for k in send_dict.keys():
                send_dict[k] = np.copy(send_dict[k][:shot_limit])


        self.send('smalldata', send_dict)


        return


    def smalldata_monitor(keys=None, shot_limit=None):
        """
        Create a monitor function to hook the publisher into the
        psana SmallData interface.

        Parameters
        ----------
        keys : list of str
            A list of the keys to send to the pubisher. `None`
            (default) indicates all should be sent.

        shot_limit : int
            The maximum number shots to keep on the redis server
            at any one time.

        Returns
        -------
        monitor_function : callable
            Returns a callable that can be passed to 
            psana.SmallData.add_monitor_function

        Example
        -------
        >>> publisher = Publisher('xpptut15')
        >>> mt_fxn = publisher.smalldata_monitor(shot_limit=1200)
        >>> smd.add_monitor_function(mt_fxn)
        """

        self.

        return monitor
        
        
if __name__ == '__main__':
    
    import time
    
    p = Publisher('experiment_number')
    
    for i in range(30):
        p.send('number', i)
        time.sleep(0.1)

    p.send('number', 'KILL')
