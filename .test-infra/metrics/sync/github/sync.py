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
'''
This module queries GitHub to collect Beam-related metrics and put them in
PostgreSQL.
'''
import itertools
import os
import socket
import sys
import time
import traceback
import re
from datetime import datetime

import requests
import psycopg2

import queries
import ghutilities


# Keeping this as reference for localhost debug
# Fetching docker host machine ip for testing purposes.
# Actual host should be used for production.
def findDockerNetworkIP():
  '''Utilizes ip tool to find docker network IP'''
  import subprocess
  cmd_out = subprocess.check_output(["ip", "route", "show"]).decode("utf-8")
  return cmd_out.split(" ")[2]


#DB_HOST = findDockerNetworkIP()

DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
DB_NAME = os.environ['DB_DBNAME']
DB_USER_NAME = os.environ['DB_DBUSERNAME']
DB_PASSWORD = os.environ['DB_DBPWD']

GH_ACCESS_TOKEN = os.environ['GH_ACCESS_TOKEN']

GH_PRS_TABLE_NAME = 'gh_pull_requests'

GH_PRS_CREATE_TABLE_QUERY = f"""
  create table {GH_PRS_TABLE_NAME} (
  pr_id integer NOT NULL PRIMARY KEY,
  author varchar NOT NULL,
  created_ts timestamp NOT NULL,
  first_non_author_activity_ts timestamp NULL,
  first_non_author_activity_author varchar NULL,
  closed_ts timestamp NULL,
  updated_ts timestamp NOT NULL,
  is_merged boolean NOT NULL,
  requested_reviewers varchar[] NOT NULL,
  beam_reviewers varchar[] NOT NULL,
  mentioned varchar[] NOT NULL,
  reviewed_by varchar[] NOT NULL
  )
  """

GH_ISSUES_TABLE_NAME = 'gh_issues'

GH_ISSUES_CREATE_TABLE_QUERY = f"""
  create table {GH_ISSUES_TABLE_NAME} (
  issue_id integer NOT NULL PRIMARY KEY,
  author varchar NOT NULL,
  created_ts timestamp NOT NULL,
  updated_ts timestamp NOT NULL,
  closed_ts timestamp NULL,
  title varchar NOT NULL,
  assignees varchar[] NOT NULL,
  labels varchar[] NOT NULL
  )
  """

GH_SYNC_METADATA_TABLE_NAME = 'gh_sync_metadata'
GH_SYNC_METADATA_TABLE_CREATE_QUERY = f"""
  create table {GH_SYNC_METADATA_TABLE_NAME} (
  name varchar NOT NULL PRIMARY KEY,
  timestamp timestamp NOT NULL
  )
  """


def initDBConnection():
  '''Opens connection to postgresql DB, as configured via global variables.'''
  conn = None
  while not conn:
    try:
      conn = psycopg2.connect(
          f"dbname='{DB_NAME}' user='{DB_USER_NAME}' host='{DB_HOST}'"
          f" port='{DB_PORT}' password='{DB_PASSWORD}'")
    except:
      print('Failed to connect to DB; retrying in 1 minute')
      sys.stdout.flush()
      time.sleep(60)
  return conn


def tableExists(cursor, tableName):
  '''Checks the existense of table.'''
  cursor.execute(f"select * from information_schema.tables"
                 f" where table_name='{tableName}';")
  return bool(cursor.rowcount)


def initDbTablesIfNeeded():
  '''Creates and initializes DB tables required for script to work.'''
  connection = initDBConnection()
  cursor = connection.cursor()

  prsTableExists = tableExists(cursor, GH_PRS_TABLE_NAME)
  print('PRs table exists', prsTableExists)
  if not prsTableExists:
    cursor.execute(GH_PRS_CREATE_TABLE_QUERY)
    if not bool(cursor.rowcount):
      raise Exception(f"Failed to create table {GH_PRS_TABLE_NAME}")

  issuesTableExists = tableExists(cursor, GH_ISSUES_TABLE_NAME)
  print('Issues table exists', issuesTableExists)
  if not issuesTableExists:
    cursor.execute(GH_ISSUES_CREATE_TABLE_QUERY)
    if not bool(cursor.rowcount):
      raise Exception(f"Failed to create table {GH_ISSUES_TABLE_NAME}")

  metadataTableExists = tableExists(cursor, GH_SYNC_METADATA_TABLE_NAME)
  print('Metadata table exists', metadataTableExists)
  if not metadataTableExists:
    cursor.execute(GH_SYNC_METADATA_TABLE_CREATE_QUERY)
    if not bool(cursor.rowcount):
      raise Exception(f"Failed to create table {GH_SYNC_METADATA_TABLE_NAME}")

  cursor.close()
  connection.commit()

  connection.close()


# TODO: Remove this logic once the gh_issue_sync row has been populated and update the fallback to datetime(year=1980, month=1, day=1)
def fetchLastSyncTimestampFallback(cursor):
  '''Fetches last sync timestamp from metadata DB table.'''
  fetchQuery = f'''
  SELECT timestamp
  FROM {GH_SYNC_METADATA_TABLE_NAME}
  WHERE name LIKE 'gh_sync'
  '''

  cursor.execute(fetchQuery)
  queryResult = cursor.fetchone()

  defaultResult = datetime(year=1980, month=1, day=1)
  return defaultResult if queryResult is None else queryResult[0]


def fetchLastSyncTimestamp(cursor, name):
  '''Fetches last sync timestamp from metadata DB table.'''
  fetchQuery = f'''
  SELECT timestamp
  FROM {GH_SYNC_METADATA_TABLE_NAME}
  WHERE name LIKE '{name}'
  '''

  cursor.execute(fetchQuery)
  queryResult = cursor.fetchone()

  return None if queryResult is None else queryResult[0]


def updateLastSyncTimestamp(timestamp, name):
  '''Updates last sync timestamp in metadata DB table.'''
  connection = initDBConnection()
  cursor = connection.cursor()

  insertTimestampSqlQuery = f'''INSERT INTO {GH_SYNC_METADATA_TABLE_NAME}
                                  (name, timestamp)
                                VALUES ('{name}', %s) 
                                ON CONFLICT (name) DO UPDATE
                                  SET timestamp = excluded.timestamp
                                '''
  cursor.execute(insertTimestampSqlQuery, [timestamp])

  cursor.close()
  connection.commit()
  connection.close()


def executeGHGraphqlQuery(query):
  '''Runs graphql query on GitHub.'''
  url = 'https://api.github.com/graphql'
  headers = {'Authorization': f'Bearer {GH_ACCESS_TOKEN}'}
  r = requests.post(url=url, json={'query': query}, headers=headers)
  return r.json()


def fetchGHData(timestamp, ghQuery):
  '''Fetches GitHub data required for reporting Beam metrics'''
  tsString = ghutilities.datetimeToGHTimeStr(timestamp)
  query = ghQuery.replace('<TemstampSubstitueLocation>', tsString)
  return executeGHGraphqlQuery(query)

def extractUserLogin(user):
  # user could be missing
  if not user:
    return "Unknown"
  return user.get("login", "Unknown")

def extractRequestedReviewers(pr):
  reviewEdges = pr["reviewRequests"]["edges"]
  return list(
      map(lambda x: extractUserLogin(x["node"]["requestedReviewer"]), reviewEdges))


def extractMentions(pr):
  body = pr["body"]
  commentEdges = pr["comments"]["edges"]
  reviewEdges = pr["reviews"]["edges"]

  bodyMentions = ghutilities.findMentions(body)
  commentMentionsLists = map(
      lambda x: ghutilities.findMentions(x["node"]["body"]), commentEdges)
  reviewMentionsLists = map(
      lambda x: ghutilities.findMentions(x["node"]["body"]), reviewEdges)
  commentMentions = [
      item for sublist in commentMentionsLists for item in sublist
  ]
  reviewMentions = [item for sublist in reviewMentionsLists for item in sublist]

  mentionsSet = set(bodyMentions) | set(commentMentions) | set(reviewMentions)
  return list(mentionsSet)


def extractFirstNAActivity(pr):
  '''
  Returns timestamp and login of author on first activity on pull request done
  by non-author.
  '''
  author = extractUserLogin(pr["author"])
  commentEdges = None
  commentEdges = [
      edge for edge in pr["comments"]["edges"]
      if extractUserLogin(edge["node"]["author"]) != author
  ]
  reviewEdges = [
      edge for edge in pr["reviews"]["edges"]
      if extractUserLogin(edge["node"]["author"]) != author
  ]
  merged = pr["merged"]
  mergedAt = pr["mergedAt"]
  mergedBy = None if not merged else extractUserLogin(pr["mergedBy"])
  commentTimestamps = list(
      map(lambda x: (x["node"]["createdAt"], extractUserLogin(x["node"]["author"])),
          commentEdges))
  reviewTimestamps = list(
      map(lambda x: (x["node"]["createdAt"], extractUserLogin(x["node"]["author"])),
          reviewEdges))
  allTimestamps = commentTimestamps + reviewTimestamps
  if merged:
    allTimestamps.append((mergedAt, mergedBy))
  return (None, None) if not allTimestamps else min(
      allTimestamps, key=lambda t: t[0])


def extractBeamReviewers(pr):
  '''Extract logins of users defined by Beam as reviewers.'''
  author = extractUserLogin(pr['author'])

  # All the direct GitHub indicators of reviewers
  reviewers = []
  for r in pr['assignees']['edges']:
    reviewers.append(extractUserLogin(r['node']))
  for r in pr['reviewRequests']['edges']:
    reviewers.append(extractUserLogin(r['node']['requestedReviewer']))

  # GitHub users that have performed reviews.
  for r in pr['reviews']['edges']:
    reviewers.append(extractUserLogin(r['node']['author']))

  # @r1, @r2 ... look/PTAL/ptal?
  beam_reviewer_regex = r'(@\w+).*?(?:PTAL|ptal|look)'
  # R= @r1 @r2 @R3
  contrib_reviewer_regex = r'(?:^|\W)[Rr]\s*[=:.]((?:[\s,;.]*-?@\w+)+)'
  username_regex = r'(-?)(@\w+)'
  for m in [pr['body']] + [c['node']['body'] for c in pr['comments']['edges']]:
    if m is None:
      continue
    for match in itertools.chain(
        re.finditer(contrib_reviewer_regex, m), re.finditer(beam_reviewer_regex, m)):
      for user in re.finditer(username_regex, match.groups()[0]):
        # First group decides if it is additive or subtractive
        remove = user.groups()[0] == '-'
        # [1:] to drop the @
        r = user.groups()[1][1:]
        if remove and r in reviewers:
          reviewers.remove(r)
        elif r not in reviewers:
          reviewers.append(r)
  return [r for r in set(reviewers) if r != author]


def extractReviewers(pr):
  '''Extracts reviewers logins from PR.'''
  return [extractUserLogin(edge["node"]["author"]) for edge in pr["reviews"]["edges"]]


def extractRowValuesFromPr(pr):
  '''
  Extracts row values required to fill Beam metrics table from PullRequest
  GraphQL response.
  '''
  requestedReviewers = extractRequestedReviewers(pr)
  mentions = extractMentions(pr)
  firstNAActivity, firstNAAAuthor = extractFirstNAActivity(pr)
  beamReviewers = extractBeamReviewers(pr)
  reviewedBy = extractReviewers(pr)

  result = [
      pr["number"], extractUserLogin(pr["author"]), pr["createdAt"], pr["updatedAt"],
      pr["closedAt"], pr["merged"], firstNAActivity, firstNAAAuthor,
      requestedReviewers, mentions, beamReviewers, reviewedBy
  ]

  return result


def extractRowValuesFromIssue(issue):
  '''
  Extracts row values required to fill Beam metrics table from PullRequest
  GraphQL response.
  '''
  assignees = []
  for a in issue['assignees']['edges']:
    assignees.append(extractUserLogin(a['node']))
  labels = []
  for l in issue['labels']['edges']:
    labels.append(l['node']['name'])

  result = [
      issue["number"], extractUserLogin(issue["author"]), issue["createdAt"], issue["updatedAt"],
      issue["closedAt"], issue["title"], assignees, labels
  ]

  return result


def upsertIntoPRsTable(cursor, values):
  upsertPRRowQuery = f'''INSERT INTO {GH_PRS_TABLE_NAME}
                            (pr_id,
                            author,
                            created_ts,
                            updated_ts,
                            closed_ts,
                            is_merged,
                            first_non_author_activity_ts,
                            first_non_author_activity_author,
                            requested_reviewers,
                            mentioned,
                            beam_reviewers,
                            reviewed_by)
                          VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                          ON CONFLICT (pr_id) DO UPDATE
                            SET
                            pr_id=excluded.pr_id,
                            author=excluded.author,
                            created_ts=excluded.created_ts,
                            updated_ts=excluded.updated_ts,
                            closed_ts=excluded.closed_ts,
                            is_merged=excluded.is_merged,
                            first_non_author_activity_ts=excluded.first_non_author_activity_ts,
                            requested_reviewers=excluded.requested_reviewers,
                            mentioned=excluded.mentioned,
                            beam_reviewers=excluded.beam_reviewers,
                            reviewed_by=excluded.reviewed_by
                          '''
  cursor.execute(upsertPRRowQuery, values)


def upsertIntoIssuesTable(cursor, values):
  upsertPRRowQuery = f'''INSERT INTO {GH_ISSUES_TABLE_NAME}
                            (issue_id,
                            author,
                            created_ts,
                            updated_ts,
                            closed_ts,
                            title,
                            assignees,
                            labels)
                          VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s)
                          ON CONFLICT (issue_id) DO UPDATE
                            SET
                            issue_id=excluded.issue_id,
                            author=excluded.author,
                            created_ts=excluded.created_ts,
                            updated_ts=excluded.updated_ts,
                            closed_ts=excluded.closed_ts,
                            title=excluded.title,
                            assignees=excluded.assignees,
                            labels=excluded.labels
                          '''
  cursor.execute(upsertPRRowQuery, values)


def fetchNewData():
  '''
  Main workhorse method. Fetches data from GitHub and puts it in metrics table.
  '''
  for i in range(2):
    kind = 'issue'
    if i == 0:
      kind = 'pr'

    connection = initDBConnection()
    cursor = connection.cursor()
    lastSyncTimestamp = fetchLastSyncTimestamp(cursor, f'gh_{kind}_sync')
    cursor.close()
    connection.close()
    if lastSyncTimestamp is None:
      if kind == 'pr':
        connection = initDBConnection()
        cursor = connection.cursor()
        lastSyncTimestamp = fetchLastSyncTimestampFallback(cursor)
        cursor.close()
        connection.close()
      else:
        lastSyncTimestamp = datetime(year=1980, month=1, day=1)

    currTS = lastSyncTimestamp

    resultsPresent = True
    while resultsPresent:
      print(f'Syncing data for {kind}s: ', currTS)
      query = queries.MAIN_PR_QUERY if kind == 'pr' else queries.MAIN_ISSUES_QUERY
      jsonData = fetchGHData(currTS, query)

      connection = initDBConnection()
      cursor = connection.cursor()

      if "errors" in jsonData:
        print("Failed to fetch data, error:", jsonData)
        return

      data = None
      try:
        data = jsonData["data"]["search"]["edges"]
      except:
        # TODO This means that API returned error.
        # We might want to bring this to stderr or utilize other means of logging.
        # Examples: we hit throttling, etc
        print("Got bad json format: ", jsonData)
        return

      if not data:
        resultsPresent = False

      for edge in data:
        node = edge["node"]
        try:
          rowValues = extractRowValuesFromPr(node) if kind == 'pr' else extractRowValuesFromIssue(node)
        except Exception as e:
          print("Failed to extract data. Exception: ", e, f" {kind}: ", edge)
          traceback.print_tb(e.__traceback__)
          return

        if kind == 'pr':
          upsertIntoPRsTable(cursor, rowValues)
        else:
          upsertIntoIssuesTable(cursor, rowValues)

        updateTime = ghutilities.datetimeFromGHTimeStr(node["updatedAt"])

        currTS = currTS if currTS > updateTime else updateTime

      cursor.close()
      connection.commit()
      connection.close()

      updateLastSyncTimestamp(currTS, f'gh_{kind}_sync')


def probeGitHubIsUp():
  '''
  Returns True if GitHub responds to simple queries. Else returns False.
  '''
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  result = sock.connect_ex(('github.com', 443))
  return True if result == 0 else False


################################################################################
if __name__ == '__main__':
  '''
  This script is supposed to be invoked directly.
  However for testing purposes and to allow importing,
  wrap work code in module check.
  '''
  print("Started.")

  print("Checking if DB needs to be initialized.")
  sys.stdout.flush()
  initDbTablesIfNeeded()

  while True:
    print("Start PR and Issue fetching.")
    sys.stdout.flush()

    if not probeGitHubIsUp():
      print("GitHub is unavailable, skipping fetching data.")
      continue
    else:
      print("GitHub is available start fetching data.")
      fetchNewData()
      print("Fetched data.")
    print("Sleeping for 5 minutes.")
    sys.stdout.flush()
    time.sleep(5 * 60)

  print('Done.')
