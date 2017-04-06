# pscache
A "live" hub for intermediate LCLS analysis results 

### Quick Start
* python setup.py install
* sh local_redis
* python animated_pub.py
* open the rerun.ipynb notebook

must have redis, redis-py, jupyter installed

### Issues to resolve
* how to serialize data for python-redis communication
* how to organize data in redis (instances vs. dbs)

### Info on Redis
* https://jira.slac.stanford.edu/browse/PSDH-71?jql=text%20~%20%22redis%22

### Schema

Each experiment will recieve it's own REDIS DB. Within that
DB, there will be the following key:value pairs


   key        value type      contents        meta/data
   ---        ----------      --------        ---------
-  runs              set    list of runs       metadata
-  run(#):keys      hash    key:"shape-type"   metadata
-  run#:(key)       list    list of data           data

This way the user can:

1. Query what runs are avaliable ("runs")
2. Query what keys are avaliable for that run ("run(#):keys")
3. Get the expected shape and type for a key ("run(#):keys" "key")
4. Get the data for a specific key ("run#:(key)")


