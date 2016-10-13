#!/usr/bin/python
import sys
import requests
import json
import kerberos

"""
Queries the YARN Job History Server (JHS) for a given jobId and returns the Hive counters
Works with unauthenticated and SPNEGO (Kerberos) authenticated API endpoints
"""

__author__      = "Jim Halfpenny"
__copyright__   = "Copyright 2016, Jim Halfpenny"
__license__ = "Apache License Version 2.0"

# Global configuration
hostname="db-secure.local" # Hostname of job history server
port="19888" # Job history server port number

# getNegotiateString
# Returns the Negotiate header payload for SPNEGO authentication
def getNegotiateString(service, hostname):
  negotiate = None
  __, krb_context = kerberos.authGSSClientInit("%s@%s" % (service, hostname))
  kerberos.authGSSClientStep(krb_context, "")
  return kerberos.authGSSClientResponse(krb_context)

# getHttpResponse
# Attempts an unauthenticated call to url, then attempts SPNEGO auth if required
# This does not attempt a Kerberos login, you need to kinit before running this script
def getHttpResponse(url):
  response = requests.get(url)
  # Check to see if the API endpoint requires Kerberos (SPNEGO) authentication
  if (response.status_code == 401 and response.headers["www-authenticate"].startswith("Negotiate")):
    # SPNEGO authentication required, let's get a HTTP ticket for the API
    negotiateString = getNegotiateString("HTTP", hostname)
    if (negotiateString == None):
      sys.stderr.write("Error: Unable to get Kerberos authentication header. Did you kinit?")
    # Build a new HTTP response using SPNEGO
    headers = {"Authorization": "Negotiate " + negotiateString}
    response = requests.get(url, headers=headers)
  return response

# getHiveCounters
# Extracts the Hive counters from the JSON received from the job history server
def getHiveCounters(jData):
  hiveCounters = None
  for counterGroup in jData['jobCounters']['counterGroup']:
    if counterGroup['counterGroupName'] == "HIVE":
      hiveCounters = counterGroup['counter']
  return hiveCounters

def main():
  if len(sys.argv) != 2:
    sys.stderr.write("Usage: get_counters.py <job_id>")
    exit(1)

  # The script takes one argument, a YARN jobId for a Hive job
  jobIds=sys.argv[1]
  
  
  allMetrics = {}
  allMetrics['hiveJobCounters'] = []
  for jobId in jobIds.split(","):
    url = 'http://%s:%s/ws/v1/history/mapreduce/jobs/%s/counters' % (hostname, port, jobId)
    response = getHttpResponse(url)

    # We should either have a non-secure or a SPNEGO response object at this point
    if (response.status_code != 200):
      # A 404 response indicates the jobId was not found on the server
      if (response.status_code == 404):
        sys.stderr.write("Error: jobId %s not found on job history server" % (jobId))
      else:
        sys.stderr.write("HTTP %d: Unable to get counters" % (response.status_code))
      exit(1)

    jData = json.loads(response.content)
    hiveCounters = getHiveCounters(jData)
    if (hiveCounters == None):
      sys.stderr.write("No Hive counters in job output, was %s really a Hive job?" % jobId)
      exit(2)

    metrics = {}
    counters = {}
    metrics['jobId'] = jobId
    for counter in hiveCounters:
      counters[counter['name']] = counter['totalCounterValue']
    
    metrics['jobId'] = jobId
    metrics['counters'] = counters
    allMetrics['hiveJobCounters'].append(metrics)

  result = "metrics=" + str(allMetrics) + "\n"

  # We can log the result to a file
  f = open('/tmp/output.txt', 'a') 
  f.write(result)
  f.close()
  
  # Alternatively we can write the result to stdout
  sys.stdout.write(result)

if __name__ == '__main__': main()
