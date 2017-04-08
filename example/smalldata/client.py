
from pscache import client
from time import sleep

c = client.ExptClient('xpptut15', host='psdb3')

sleep(5)

print c.runs()
print c.keyinfo(54)

while True:
    d = c.fetch_data(54, keys=['event_time'])
    print len(d['event_time']), 'events in DB'
    sleep(1)

