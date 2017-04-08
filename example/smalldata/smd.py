
import psana
import numpy as np

dsource = psana.MPIDataSource('exp=xpptut15:run=54:smd')
dsource.break_after(10)

smldata = dsource.small_data('run54.h5', gather_interval=3)
smldata.connect_redis() # *** NEW ***

cspad = psana.Detector('cspad')

for nevt,evt in enumerate(dsource.events()):
    print 'smd event:', nevt
    img = cspad.image(evt)
    if img is not None:
        smldata.event(cspad=img)


smldata.save()
print 'done'

