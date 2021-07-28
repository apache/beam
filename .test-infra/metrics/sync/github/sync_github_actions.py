#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''
This module queries GitHub to collect pre-commit GitHub Actions metrics and put them in
PostgreSQL.
'''
import sys
import os
import time
import psycopg2
import datetime
import argparse

# Instructions to run this directly from the command line.
parser = argparse.ArgumentParser(description='Saves status of GitHub Actions jobs into a postgresql database.')
parser.add_argument('job_name', help='Name of the job to be saved.')
parser.add_argument('status', help='Whether or not the job succeeded or was skipped.', choices=['success', 'failure', 'skipped'])
parser.add_argument('workflow_id', help='Used to access the objects of the current thread of execution.')
parser.parse_args()

MAXIMUM_CONN_ATTEMPTS = 5

# Keeping this as reference for localhost debug
# Fetching docker host machine ip for testing purposes.
# Actual host should be used for production.
def findDockerNetworkIP():
  '''Utilizes ip tool to find docker network IP'''
  import subprocess
  cmd_out = subprocess.check_output(["ip", "route", "show"]).decode("utf-8")
  return cmd_out.split(" ")[2]


#DB_HOST = findDockerNetworkIP()

GH_PRS_TABLE_NAME = 'gh_actions_metrics'

GH_PRS_CREATE_TABLE_QUERY = f"""
  create table {GH_PRS_TABLE_NAME} (
  ga_id serial NOT NULL PRIMARY KEY,
  job_name varchar NOT NULL,
  status varchar NOT NULL,
  workflow_id varchar NOT NULL,
  workflow_url varchar NOT NULL,
  executed_ts timestamp NOT NULL
  )
  """

def initDBConnection():
  '''Opens connection to postgresql DB, as configured via global variables.'''

  DB_HOST = os.environ['DB_HOST']
  DB_PORT = os.environ['DB_PORT']
  DB_NAME = os.environ['DB_DBNAME']
  DB_USER_NAME = os.environ['DB_DBUSERNAME']
  DB_PASSWORD = os.environ['DB_DBPWD']

  conn = None
  conn_attempts = 0
  while conn_attempts < MAXIMUM_CONN_ATTEMPTS:
    try:
      conn = psycopg2.connect(
          f"dbname='{DB_NAME}' user='{DB_USER_NAME}' host='{DB_HOST}'"
          f" port='{DB_PORT}' password='{DB_PASSWORD}'")
    except:
      remaining_attempts = MAXIMUM_CONN_ATTEMPTS - conn_attempts
      print('Failed to connect to DB; retrying in one minute. Will cease after %s attempt(s)' % remaining_attempts)
      conn_attempts += 1
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

  if connection is None:
    print('Connection to DB not established successfully. Terminating script.')
    sys.exit()

  cursor = connection.cursor()

  buildsTableExists = tableExists(cursor, GH_PRS_TABLE_NAME)
  print('PRs table %s exists? %s' % (GH_PRS_TABLE_NAME, buildsTableExists))
  if not buildsTableExists:
    print('Creating table')
    cursor.execute(GH_PRS_CREATE_TABLE_QUERY)
    if not bool(cursor.rowcount):
      raise Exception(f"Failed to create table {GH_PRS_TABLE_NAME}")
  cursor.close()
  connection.commit()
  connection.close()

def insertIntoTable(cursor, values):
  insertRowQuery = f'''INSERT INTO {GH_PRS_TABLE_NAME}
                            (job_name,
                            status,
                            workflow_id,
                            workflow_url,
                            executed_ts)
                          VALUES
                            (%s, %s, %s, %s, %s)
                          '''
  cursor.execute(insertRowQuery, values)

def collectNewData():
  '''
  Main workhorse method. Extracts the data provided and puts it in metrics table.
  '''

  job_name = sys.argv[1]
  status = sys.argv[2]
  workflow_id = sys.argv[3]
  workflow_url = f'''https://github.com/apache/beam/actions/runs/{workflow_id}'''
  executed_ts = datetime.datetime.now()
  row_values = [job_name, status, workflow_id, workflow_url, executed_ts]
  connection = initDBConnection()
  cursor = connection.cursor()
  insertIntoTable(cursor, row_values)
  cursor.close()
  connection.commit()
  connection.close()

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

  print("Collecting Actions metrics.")
  sys.stdout.flush()

  collectNewData()
  print("Fetched metrics.")

  print('Done.')
