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

# Queries Jira to collect metrics and put them in postgresql.
import os
import psycopg2
import re
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

jiraIssuesTableName = 'jira_issues'

jiraIssuesCreateTableQuery = f"""
create table {jiraIssuesTableName} (
id integer PRIMARY KEY,
key varchar,
creator varchar,
assignee varchar,
status varchar,
labels varchar,
summary varchar,
created TIMESTAMP,
resolutiondate TIMESTAMP)
"""

jiraIssuesMetadataTableName = 'jira_issues_metadata'

jiraIssuesMetadataCreateTableQuery = f"""
create table {jiraIssuesMetadataTableName} (
lastsynctime TIMESTAMP
)
"""

def fetchIssues(startTime, startAt = 0):
  startTimeStr = startTime.strftime("%Y-%m-%d %H:%M")
  # time format 0001-01-01 00:01   ==> "yyyy-mm-dd hh:mm"
  url = (f'https://issues.apache.org/jira/rest/api/latest/search?'
         f'jql=project=BEAM '
         f'AND updated > "{startTimeStr} "'
         f'AND component=test-failures'
         f'&maxResults=100&startAt={startAt}')
  print(url)

  r = requests.get(url)
  return r.json()


def initDBConnection():
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


def initDBTablesIfNeeded():
  connection = initDBConnection()
  cursor = connection.cursor()

  buildsTableExists = tableExists(cursor, jiraIssuesTableName)
  print('Builds table exists', buildsTableExists)
  if not buildsTableExists:
    cursor.execute(jiraIssuesCreateTableQuery)
    if not bool(cursor.rowcount):
      raise Exception(f"Failed to create table {jiraIssuesTableName}")

  metadataTableExists = tableExists(cursor, jiraIssuesMetadataTableName)
  print('Metadata table exists', buildsTableExists)
  if not metadataTableExists:
    cursor.execute(jiraIssuesMetadataCreateTableQuery)
    if not bool(cursor.rowcount):
      raise Exception(f"Failed to create table {jiraIssuesMetadataTableName}")

  minTimestamp = datetime(1970, 1, 1)
  insertDummyTimestampSqlQuery = f"insert into {jiraIssuesMetadataTableName} values (%s)"

  cursor.execute(insertDummyTimestampSqlQuery, [minTimestamp])

  cursor.close()
  connection.commit()

  connection.close()


def updateLastSyncTimestamp(timestamp):
  connection = initDBConnection()
  cursor = connection.cursor()

  cleanupQuery = f"delete from {jiraIssuesMetadataTableName}"
  cursor.execute(cleanupQuery)
  insertTimestampSqlQuery = f"insert into {jiraIssuesMetadataTableName} values (%s)"
  cursor.execute(insertTimestampSqlQuery, [timestamp])

  cursor.close()
  connection.commit()
  connection.close()


def fetchLastSyncTime():
  connection = initDBConnection()
  cursor = connection.cursor()

  fetchQuery = f'select lastsynctime from {jiraIssuesMetadataTableName}'

  cursor.execute(fetchQuery)
  result = cursor.fetchone()[0]

  cursor.close()
  connection.close()
  return result


def buildRowValuesArray(issue):
  fields = issue['fields']
  values = [issue['id'],
            issue['key'],
            fields['creator']['name'],
            fields['assignee']['name'] if fields['assignee'] is not None else None,
            fields['status']['name'],
            fields['labels'],
            fields['summary'],
            fields['created'],
            fields['resolutiondate']
            ]
  return values


def insertRow(cursor, rowValues):
  insertClause = (f'''insert into {jiraIssuesTableName}
                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO UPDATE
                     set
                       id = excluded.id,
                       key = excluded.key,
                       creator = excluded.creator,
                       assignee = excluded.assignee,
                       status = excluded.status,
                       labels = excluded.labels,
                       summary = excluded.summary,
                       created = excluded.created,
                       resolutiondate = excluded.resolutiondate'''
  )

  cursor.execute(insertClause, rowValues)


def fetchNewData():
  currentTimestamp = datetime.now()
  lastSyncTimestamp = fetchLastSyncTime()

  startAt = 0
  total = 1

  while (startAt < total):
    queryResult = fetchIssues(lastSyncTimestamp, startAt)

    newIssues = queryResult['issues']
    fetchedCount = len(newIssues)

    startAt += fetchedCount
    total = queryResult['total']

    connection = initDBConnection()
    cursor = connection.cursor()

    for issue in newIssues:
      rowValues = buildRowValuesArray(issue)
      insertRow(cursor, rowValues)

    cursor.close()
    connection.commit()
    connection.close()

  updateLastSyncTimestamp(currentTimestamp)
  lastSyncTimestamp = fetchLastSyncTime()


def probeJiraIsUp():
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

  # This is close enough for Jira
  result = sock.connect_ex(('issues.apache.org', 443))
  return True if result == 0 else False


################################################################################
if __name__ == '__main__':
  print("Started.")

  print("Checking if DB needs to be initialized.")
  sys.stdout.flush()
  initDBTablesIfNeeded()

  print("Start jobs fetching loop.")
  sys.stdout.flush()

  while True:
    if not probeJiraIsUp():
      print("Jira is unavailable, skipping fetching data.")
      continue
    else:
      print("Start fetching data.")
      fetchNewData()
      print("Done fetching data.")
    print("Sleeping for 5 min.")
    sys.stdout.flush()
    time.sleep(5 * 60)

  print('Done.')

