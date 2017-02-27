
from pscache.publisher import Publisher
import numpy as np
import time

p = Publisher('no_experiment')
x = np.linspace(0, 2, 1000)

i = 0
while True:
	i += 1
	y = np.sin(2 * np.pi * (x - 0.1 * i))
	d = {'x' : x, 'y' : y}
	p.send('sine', d)
	print i
	time.sleep(1)
