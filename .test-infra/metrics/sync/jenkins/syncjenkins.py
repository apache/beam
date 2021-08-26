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
import os
import psycopg2
import requests
import socket
import sys
import time

from datetime import datetime, timedelta
from xml.etree import ElementTree

# Keeping this as reference for localhost debug
# Fetching docker host machine ip for testing purposes.
# Actual host should be used for production.
# import subprocess
# cmd_out = subprocess.check_output(["ip", "route", "show"]).decode("utf-8")
# host = cmd_out.split(" ")[2]

host = os.environ['DB_HOST']
port = os.environ['DB_PORT']
dbname = os.environ['DB_DBNAME']
dbusername = os.environ['DB_DBUSERNAME']
dbpassword = os.environ['DB_DBPWD']


jenkinsBuildsTableName = 'jenkins_builds'

jenkinsJobsCreateTableQuery = f"""
create table {jenkinsBuildsTableName} (
job_name varchar NOT NULL,
build_id integer NOT NULL,
build_url varchar,
build_result varchar,
build_timestamp TIMESTAMP,
build_builtOn varchar,
build_duration BIGINT,
build_estimatedDuration BIGINT,
build_fullDisplayName varchar,
timing_blockedDurationMillis BIGINT,
timing_buildableDurationMillis BIGINT,
timing_buildingDurationMillis BIGINT,
timing_executingTimeMillis BIGINT,
timing_queuingDurationMillis BIGINT,
timing_totalDurationMillis BIGINT,
timing_waitingDurationMillis BIGINT,
primary key(job_name, build_id)
)
"""

def fetchJobs():
  url = ('https://ci-beam.apache.org/api/json'
         '?tree=jobs[name,url,lastCompletedBuild[id]]&depth=1')
  r = requests.get(url)
  jobs = r.json()[u'jobs']
  result = map(lambda x: (x['name'],
                          int(x['lastCompletedBuild']['id'])
                              if x['lastCompletedBuild'] is not None
                              else -1, x['url']), jobs)
  return result

def initConnection():
  conn = None
  while not conn:
    try:
      conn = psycopg2.connect(f"dbname='{dbname}' user='{dbusername}' host='{host}'"
                              f" port='{port}' password='{dbpassword}'")
    except:
      print('Failed to connect to DB; retrying in 1 minute')
      time.sleep(60)
  return conn

def tableExists(cursor, tableName):
  cursor.execute(f"select * from information_schema.tables"
                 f" where table_name='{tableName}';")
  return bool(cursor.rowcount)


def initDbTablesIfNeeded():
  connection = initConnection()
  cursor = connection.cursor()

  buildsTableExists = tableExists(cursor, jenkinsBuildsTableName)
  print('Builds table exists', buildsTableExists)
  if not buildsTableExists:
    cursor.execute(jenkinsJobsCreateTableQuery)
    if not bool(cursor.rowcount):
      raise Exception(f"Failed to create table {jenkinsBuildsTableName}")

  cursor.close()
  connection.commit()

  connection.close()

# TODO rename to fetchLastSyncJobIds
def fetchLastSyncTimestamp(cursor):
  fetchQuery = f'''
  select job_name, max(build_id)
  from {jenkinsBuildsTableName}
  group by job_name
  '''

  cursor.execute(fetchQuery)
  return dict(cursor.fetchall())


def fetchBuildsForJob(jobUrl):
  durFields = ('blockedDurationMillis,buildableDurationMillis,'
               'buildingDurationMillis,executingTimeMillis,queuingDurationMillis,'
               'totalDurationMillis,waitingDurationMillis')
  fields = (f'result,timestamp,id,url,builtOn,building,duration,'
            f'estimatedDuration,fullDisplayName,actions[{durFields}]')
  url = f'{jobUrl}api/json?depth=1&tree=builds[{fields}]'
  r = requests.get(url)
  return r.json()[u'builds']


def buildRowValuesArray(jobName, build):
  timings = next((x
                  for x in build[u'actions']
                  if (u'_class' in x)
                    and (x[u'_class'] == u'jenkins.metrics.impl.TimeInQueueAction')),
                 None)
  values = [jobName,
          int(build[u'id']),
          build[u'url'],
          build[u'result'],
          datetime.fromtimestamp(build[u'timestamp'] / 1000),
          build[u'builtOn'],
          build[u'duration'],
          build[u'estimatedDuration'],
          build[u'fullDisplayName'],
          timings[u'blockedDurationMillis'] if timings is not None else -1,
          timings[u'buildableDurationMillis'] if timings is not None else -1,
          timings[u'buildingDurationMillis'] if timings is not None else -1,
          timings[u'executingTimeMillis'] if timings is not None else -1,
          timings[u'queuingDurationMillis'] if timings is not None else -1,
          timings[u'totalDurationMillis'] if timings is not None else -1,
          timings[u'waitingDurationMillis'] if timings is not None else -1]
  return values


def insertRow(cursor, rowValues):
  cursor.execute(f'insert into {jenkinsBuildsTableName} values (%s, %s, %s, %s,'
                  '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', rowValues)


def fetchNewData():
  connection = initConnection()
  cursor = connection.cursor()
  syncedJobs = fetchLastSyncTimestamp(cursor)
  cursor.close()
  connection.close()

  newJobs = fetchJobs()

  for newJobName, newJobLastBuildId, newJobUrl in newJobs:
    syncedJobId = syncedJobs[newJobName] if newJobName in syncedJobs else -1
    if newJobLastBuildId > syncedJobId:
      builds = fetchBuildsForJob(newJobUrl)
      builds = [x for x in builds if int(x[u'id']) > syncedJobId]

      connection = initConnection()
      cursor = connection.cursor()

      for build in builds:
        if build[u'building']:
          continue;
        rowValues = buildRowValuesArray(newJobName, build)
        print("inserting", newJobName, build[u'id'])
        insertRow(cursor, rowValues)

      cursor.close()
      connection.commit()
      connection.close()  # For some reason .commit() doesn't push data

def probeJenkinsIsUp():
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  result = sock.connect_ex(('ci-beam.apache.org', 443))
  return True if result == 0 else False


################################################################################
if __name__ == '__main__':
  print("Started.")

  print("Checking if DB needs to be initialized.")
  sys.stdout.flush()
  initDbTablesIfNeeded()

  print("Start jobs fetching loop.")
  sys.stdout.flush()

  while True:
    if not probeJenkinsIsUp():
      print("Jenkins is unavailable, skipping fetching data.")
      continue
    else:
      fetchNewData()
      print("Fetched data.")
    print("Sleeping for 5 min.")
    sys.stdout.flush()
    time.sleep(5 * 60)

  print('Done.')

