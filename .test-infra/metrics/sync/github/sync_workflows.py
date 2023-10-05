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
This module queries GitHub API to collect Beam-related workflows metrics and
put them in PostgreSQL.
This script is running every 3 hours as a cloud function
"github_actions_workflows_dashboard_sync" in apache-beam-testing project:
https://console.cloud.google.com/functions/details/us-central1/github_actions_workflows_dashboard_sync?env=gen1&project=apache-beam-testing
This cloud function is triggered by a pubsub topic:
https://console.cloud.google.com/cloudpubsub/topic/detail/github_actions_workflows_sync?project=apache-beam-testing
Cron Job:
https://console.cloud.google.com/cloudscheduler/jobs/edit/us-central1/github_actions_workflows_dashboard_sync?project=apache-beam-testing
'''

import asyncio
import aiohttp
import backoff
import math
import os
import sys
import time
import re
import psycopg2

DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
DB_NAME = os.environ['DB_DBNAME']
DB_USER_NAME = os.environ['DB_DBUSERNAME']
DB_PASSWORD = os.environ['DB_DBPWD']
GH_WORKFLOWS_TABLE_NAME = "github_workflows"

GH_ACCESS_TOKEN = os.environ['GH_ACCESS_TOKEN']
GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH = 100


class Workflow:
  def __init__(self, id, name, filename):
    self.id = id
    self.name = name
    self.filename = filename
    self.run_results = ['None'] * GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH
    self.run_urls = ['None'] * GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH

GH_WORKFLOWS_CREATE_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS {GH_WORKFLOWS_TABLE_NAME} (
  workflow_id integer NOT NULL PRIMARY KEY,
  job_name text NOT NULL,
  job_yml_filename text NOT NULL"""
for i in range(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH):
  GH_WORKFLOWS_CREATE_TABLE_QUERY += f""",
  run{i+1} text,
  run{i+1}Id text"""
GH_WORKFLOWS_CREATE_TABLE_QUERY += ")\n"

def githubWorkflowsGrafanaSync(data, context):
  return asyncio.run(sync_workflow_runs())

async def sync_workflow_runs():
  print('Started')
  print('Updating table with recent workflow runs')
  databaseOperations(initDbConnection(), await fetch_workflow_data())
  print('Done')
  return "Completed"

def initDbConnection():
  '''Init connection with the Database'''
  connection = None
  maxRetries = 3
  i = 0
  while connection is None and i < maxRetries:
    try:
      connection = psycopg2.connect(
        f"dbname='{DB_NAME}' user='{DB_USER_NAME}' host='{DB_HOST}'"
        f" port='{DB_PORT}' password='{DB_PASSWORD}'")
    except Exception as e:
      print('Failed to connect to DB; retrying in 1 minute')
      print(e)
      time.sleep(60)
      i = i + 1
      if i >= maxRetries:
        print("Number of retries exceded ")
        sys.exit(1)
  return connection

# Retries 9 times: 1s, 2s, 4s, 8s, 16s, ...
@backoff.on_exception(backoff.expo, Exception, max_tries=9, factor=2)
async def fetch(url, params=None, headers=None):
  async with aiohttp.ClientSession() as session:
    async with session.get(url, params=params, headers=headers) as response:
      if response.status != 200:
        raise aiohttp.ClientResponseError(
          response.request_info,
          response.history,
          status=response.status,
          message=response.reason,
          headers=response.headers
        )
      return await response.json()

async def fetch_workflow_data():
  url = "https://api.github.com/repos/apache/beam/actions/workflows"
  headers = {'Authorization': f'Bearer {GH_ACCESS_TOKEN}'}
  page = 1
  number_of_entries_per_page = 100 # The number of results per page (max 100)
  query_options =\
    {'branch': 'master', 'page': page, 'per_page': number_of_entries_per_page}

  print("Start fetching recent workflow runs")
  workflow_tasks = []
  response = await fetch(url, query_options, headers)
  while math.ceil(response['total_count'] / number_of_entries_per_page) >= page:
    query_options = {
      'branch': 'master',
      'page': page,
      'per_page': number_of_entries_per_page
    }
    task = asyncio.ensure_future(fetch(url, query_options, headers))
    workflow_tasks.append(task)
    page += 1

  workflow_run_tasks = []
  workflows_dict = {}
  for completed_task in asyncio.as_completed(workflow_tasks):
    response = await completed_task
    workflows = response.get('workflows', [])
    for workflow in workflows:
      workflow_id = workflow['id']
      workflow_name = workflow['name']
      workflow_path = workflow['path']
      result = re.search(r'(workflows\/.*)$', workflow_path)
      if result:
        workflow_path = result.group(1)

      workflows_dict[workflow_id] =\
        Workflow(workflow_id, workflow_name, workflow_path)

      runs_url = f"{url}/{workflow_id}/runs"
      page = 1
      pages_to_fetch = math.ceil(
        GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH / number_of_entries_per_page
      )
      while pages_to_fetch >= page:
        query_options = {
          'branch': 'master',
          'page': page,
          'per_page': number_of_entries_per_page,
          'exclude_pull_requests': 'true'
        }
        task = asyncio.ensure_future(fetch(runs_url, query_options, headers))
        workflow_run_tasks.append(task)
        page += 1
  print("Successfully fetched workflow runs")

  print("Start fetching workflow run's details")
  responses = await asyncio.gather(*workflow_run_tasks)
  for response in responses:
    workflow_runs = response.get('workflow_runs', [])
    for idx, run in enumerate(workflow_runs):
      workflow = workflows_dict[run['workflow_id']]
      workflow.run_urls[idx] = run['html_url']
      if run['status'] == 'completed':
        workflow.run_results[idx] = run['conclusion']
      elif run['status'] != 'cancelled':
        workflow.run_results[idx] = run['status']
      print(f"Successfully fetched details for: {workflow.filename}")
  print("Successfully fetched workflow run's details")

  return list(workflows_dict.values())

def databaseOperations(connection, workflows):
  '''Create the table if not exist and update the table with the latest runs
  of the workflows '''
  queryInsert = f"INSERT INTO {GH_WORKFLOWS_TABLE_NAME} VALUES "
  cursor = connection.cursor()
  cursor.execute(GH_WORKFLOWS_CREATE_TABLE_QUERY)
  cursor.execute(f"DELETE FROM {GH_WORKFLOWS_TABLE_NAME};")
  query = ""
  for workflow in workflows:
    rowInsert =\
      f"(\'{workflow.id}\',\'{workflow.name}\',\'{workflow.filename}\'"
    for run, run_urls in zip(workflow.run_results, workflow.run_urls):
      rowInsert += f",\'{run}\',\'{run_urls}\'"
    query = query + rowInsert
    query += "),"
  query = query[:-1] + ";"
  query = queryInsert + query
  cursor.execute(query)
  cursor.close()
  connection.commit()
  connection.close()
