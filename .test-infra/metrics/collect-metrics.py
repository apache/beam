#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#

# Queries Jenkins to collect metrics and pu them in bigquery.
import time
import requests
from google.cloud import bigquery


schema = [
  bigquery.SchemaField('build_url', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('job_name', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('job_url', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('build_result', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('build_timestamp', 'TIMESTAMP', mode='REQUIRED'),
  bigquery.SchemaField('build_id', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('build_builtOn', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('build_duration', 'INTEGER', mode='REQUIRED'),
  bigquery.SchemaField('build_estimatedDuration', 'INTEGER', mode='REQUIRED'),
  bigquery.SchemaField('build_fullDisplayName', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('timing_blockedDurationMillis', 'INTEGER', mode='REQUIRED'),
  bigquery.SchemaField('timing_buildableDurationMillis', 'INTEGER', mode='REQUIRED'),
  bigquery.SchemaField('timing_buildingDurationMillis', 'INTEGER', mode='REQUIRED'),
  bigquery.SchemaField('timing_executingTimeMillis', 'INTEGER', mode='REQUIRED'),
  bigquery.SchemaField('timing_queuingDurationMillis', 'INTEGER', mode='REQUIRED'),
  bigquery.SchemaField('timing_totalDurationMillis', 'INTEGER', mode='REQUIRED'),
  bigquery.SchemaField('timing_waitingDurationMillis', 'INTEGER', mode='REQUIRED'),
]

def createTable(client):
  tr = client.dataset('beam_metrics').table('builds')
  table = bigquery.Table(tr, schema=schema)
  #client.delete_table(table)
  client.create_table(table)

# Collapse the timings into key values
def collapse(json):
  ret = dict()
  for jset in json:
    for k in jset:
      ret[k] = jset[k]
  if not u'blockedDurationMillis' in ret:
    print '!!!! %s' % ret
  return ret
 
def makeRowArray(job, build, timings):
  # TODO build this array from the schema.
  return [build[u'url'], job[u'name'], job[u'url'], build[u'result'], build[u'timestamp'] / 1000,
          build[u'id'],build[u'builtOn'], build[u'duration'], build[u'estimatedDuration'],
          build[u'fullDisplayName'], 
          timings[u'blockedDurationMillis'],
          timings[u'buildableDurationMillis'],
          timings[u'buildingDurationMillis'],
          timings[u'executingTimeMillis'],
          timings[u'queuingDurationMillis'],
          timings[u'totalDurationMillis'],
          timings[u'waitingDurationMillis']]

# Uses the schema to convert the data into json.
def jsonFromRow(row):
  ret = dict()
  i = 0
  for sch in schema:
    ret[sch.name] = row[i]
    i = i + 1
  return ret

# Get the list of jobs.
def getJobs():
  url = 'https://builds.apache.org/view/A-D/view/Beam/api/json?tree=jobs%5Bname,url,lastCompletedBuild%5btimestamp%5d%5D&depth=1'
  print url
  r = requests.get(url)
  return r.json()

# Get all the builds.
def getBuilds(baseUrl):
  durFields = 'blockedDurationMillis,buildableDurationMillis,buildingDurationMillis,executingTimeMillis,queuingDurationMillis,totalDurationMillis,waitingDurationMillis'
  fields = 'result,timestamp,id,url,builtOn,building,duration,estimatedDuration,fullDisplayName,actions%%5b%s%%5d' % durFields
  url = '%sapi/json?tree=builds%%5b%s%%5d&depth=1' % (baseUrl, fields)
  time.sleep(2)
  print url
  r = requests.get(url)
  return r.json()

# Store the build urls already seen to mostly avoid storing duplicates (may still happen due to bigquery caching)
buildKeyMap = dict()

# Load the data from bigQuery
def loadData(client):
  sql = 'SELECT build_url FROM beam_metrics.builds'
  data = client.query(sql)
  for row in list(data):
    buildKeyMap[row[0]] = True

# Only grabs recent jobs
def filter(job):
  if not job[u'lastCompletedBuild']:
    return False
  delta_days = (time.time() - job[u'lastCompletedBuild'][u'timestamp'] / 1000) / (24 * 3600)
  if delta_days > 7:
    print 'Skipping old build %s, %g days old' % (job[u'name'],delta_days)
    return False
  return True

# Collect all the data
def collectDataForRecentJobs(client):
  rows = []
  jobs = getJobs()
  for job in jobs[u'jobs']:
    if filter(job):
      if job[u'lastCompletedBuild']:
        delta = (time.time() - job[u'lastCompletedBuild'][u'timestamp'] / 1000)
        if delta < 7 * 24 * 3600:
          already_got = 0
          new_rows = 0
          print job[u'name']
          builds = getBuilds(job[u'url'])
          for build in builds[u'builds']:
            if build[u'building']:
              print 'In progress, skipping %s' %  build[u'url']
            elif not build[u'url'] in buildKeyMap:
              timings = collapse(build[u'actions'])
              if u'builtOn' in build and build[u'builtOn'] and 'blockedDurationMillis' in timings:
                new_rows += 1
                rows.append(makeRowArray(job, build, timings))
              else:
                print '!!! bad row' 
                print  build
            else:
              already_got += 1
          print 'Already got %d, new rows %d' % (already_got, new_rows) 
  if not rows:
    print 'No new data'
    return
  print 'Adding %d new rows' % len(rows)
  tr = client.dataset('beam_metrics').table('builds')
  table = client.get_table(tr)
  errors = client.insert_rows(table, rows)
  if errors:
    print errors
  assert [] == errors
  for row in rows:
    buildKeyMap[row[0]] = True

# Collect data and store it
client = bigquery.Client(project='apache-beam-testing')
#createTable(client)
loadData(client)
collectDataForRecentJobs(client)
print 'Done'
