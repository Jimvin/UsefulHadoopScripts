# Useful Hadoop Scripts
This is a collection of scripts for various Hadoop tasks

## getHiveCounters.py
This python script will query the Job History Server API for a given jobId and return the Hive counters for this job if available and supports SPNEGO (Kerberos) authentication.
```bash
$ ./getHiveCounters.py job_1475660288847_0030
metrics={u'RECORDS_OUT_INTERMEDIATE': 7, u'RECORDS_OUT_0': 7, u'CREATED_FILES': 1, u'RECORDS_IN': 91, u'DESERIALIZE_ERRORS': 0}
```
