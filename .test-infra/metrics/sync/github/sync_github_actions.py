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
This module queries GitHub to collect pre-commit GitHub Actions metrics and put them in
PostgreSQL.
'''
import os
import sys
import time
import psycopg2
import datetime

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

GH_PRS_TABLE_NAME = 'gh_actions_metrics'

GH_PRS_CREATE_TABLE_QUERY = f"""
  create table {GH_PRS_TABLE_NAME} (
  ga_id serial NOT NULL PRIMARY KEY,
  job_name varchar NOT NULL,
  status varchar NOT NULL,
  executed_ts timestamp NOT NULL
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

  buildsTableExists = tableExists(cursor, GH_PRS_TABLE_NAME)
  print('PRs table exists', buildsTableExists)
  if not buildsTableExists:
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
                            executed_ts)
                          VALUES
                            (%s, %s, %s)
                          '''
  cursor.execute(insertRowQuery, values)

def fetchNewData():
  '''
  Main workhorse method. Fetches data from GitHub and puts it in metrics table.
  '''

  connection = initDBConnection()
  cursor = connection.cursor()

  if len(sys.argv) < 3:
    print('Please provide the appropriate number of parameters.')
  else:
    job_name = sys.argv[1]
    status = sys.argv[2]
    executed_ts = datetime.datetime.now()
    rowValues = [job_name, status, executed_ts]
    insertIntoTable(cursor, rowValues)

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

  print("Fetching GitHub Actions metrics.")
  sys.stdout.flush()

  fetchNewData()
  print("Fetched metrics.")

  print('Done.')
