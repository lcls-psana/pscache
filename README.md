# pscache
A "live" hub for intermediate LCLS analysis results. Useful for cases where the analysis is too complex for AMI and ~2-3 min delay is acceptable. Emphasizes simple and flexible access to data.

### Schema

Each experiment will recieve it's own REDIS DB. Within that
DB, there will be the following key:value pairs


   key        value type      contents        meta/data
   ---        ----------      --------        ---------
-  run(#):keyinfo   hash      list of keys    metadata
-  run#:(key)       list      data            data

This way the user can:

1. Query what runs are avaliable ("run\*:keyinfo")
2. Query what keys are avaliable for that run ("run(#):keyinfo")
3. Get the data for a specific key ("run#:(key)")

### to do

1. test performance
2. to xarray
3. (for later) one DB per expt

