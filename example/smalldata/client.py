
from pscache import client
from time import sleep

c = client.ExptClient('xpptut15', host='psdb3')

#sleep(10)

print c.runs()
#print c.keys(9999)
#print c.keyinfo(9999)

while True:
    print c.fetch_data(9999, keys=['cspad']) # should be 54 TODO
    sleep(1)

