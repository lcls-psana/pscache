
import redis


class Publisher(object):
    
    def __init__(self, exp):
        
        # TODO map experiment to DB or Redis instance
        
        self._redis = redis.StrictRedis(host='localhost', port=6379, 
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
        
        
    def send(self, topic, message):
        self._redis.publish(topic, message)
        
        
if __name__ == '__main__':
    
    import time
    
    p = Publisher('blank')
    
    for i in range(30):
        p.send('number', {'a' : i})
        time.sleep(0.1)
    p.send('number', 'KILL')