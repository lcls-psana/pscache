
import psana
import numpy as np

from pscache import publisher

dsource = psana.MPIDataSource('exp=xpptut15:run=54:smd')
dsource.break_after(110)

smldata = dsource.small_data('run54.h5', gather_interval=3)
pub = publisher.ExptPublisher('xpptut15')
smldata.add_monitor_function(pub.smalldata_monitor())

cspad = psana.Detector('cspad')

for nevt,evt in enumerate(dsource.events()):
	img = cspad.image(evt)
	if img is not None:
    	smldata.event(cspad=img)

smldata.save()
print 'done'

