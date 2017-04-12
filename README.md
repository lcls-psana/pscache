# pscache
A "live" hub for intermediate LCLS analysis results 

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

### to do

1a. data lifetime
1b. run length
2. to xarray
3. flush/delete
4. (for later) one DB per expt

