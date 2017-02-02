
import redis
import cPickle
import threading
import time

class Client(threading.Thread):
    
    def __init__(self, topic):
        
        threading.Thread.__init__(self)
        
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
        self._pubsub.subscribe(topic)
        
        return
        
        
    def query(self):
        
        item = self._pubsub.get_message()
        if item is not None:
            if item['type'] == 'message':
                #print item['channel'], ":", item['data']
                d = cPickle.loads(item['data'])
            else:
                self.query()
        else:
            # messages are also sent to indicate pub/subs etc
            # this recursion ensures we get a message
            self.query()
        
        return d
    
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
        
        
                
if __name__ == '__main__':
    
    c = Client('number')
    
    time.sleep(1)
    
    print '---- single queries ----'
    c.query()
    c.query()
    c.query()
    
    print '----   following    ----'
    c.follow()
    