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
from github import GithubIntegration

DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
DB_NAME = os.environ['DB_DBNAME']
DB_USER_NAME = os.environ['DB_DBUSERNAME']
DB_PASSWORD = os.environ['DB_DBPWD']
GH_APP_ID = os.environ['GH_APP_ID']
GH_APP_INSTALLATION_ID = os.environ['GH_APP_INSTALLATION_ID']
GH_PEM_KEY = os.environ['GH_PEM_KEY']
GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH =\
  os.environ['GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH']


class Workflow:
  def __init__(self, id, name, filename):
    self.id = id
    self.name = name
    self.filename = filename
    self.runs = []

def get_dashboard_category(workflow_name):
  workflow_name = workflow_name.lower()
  lang = ''
  if 'java' in workflow_name:
    lang = 'java'
  elif 'python' in workflow_name:
    lang = 'python'
  elif 'go' in workflow_name:
    lang = 'go'
  else:
    # If no language found, toss in miscellaneous bucket
    return 'misc'
  if 'dataflow' in workflow_name:
    return f'dataflow_{lang}'
  if 'spark' in workflow_name:
    return f'spark_{lang}'
  if 'flink' in workflow_name:
    return f'flink_{lang}'
  return f'core_{lang}'

async def github_workflows_dashboard_sync():
  print('Started')
  print('Updating table with recent workflow runs')

  if not GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH or \
    not GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH.isdigit():
    raise ValueError(
      'The number of workflow runs to fetch is not specified or not an integer'
    )

  database_operations(init_db_connection(), await fetch_workflow_data())

  print('Done')
  return "Completed"

def init_db_connection():
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

def get_token():
  git_integration = GithubIntegration(GH_APP_ID, GH_PEM_KEY)
  token = git_integration.get_access_token(GH_APP_INSTALLATION_ID).token
  return f'Bearer {token}'
  
@backoff.on_exception(backoff.constant, aiohttp.ClientResponseError, max_tries=5)
async def fetch(url, semaphore, params=None, headers=None, request_id=None):
  async with semaphore:
    async with aiohttp.ClientSession() as session:
      async with session.get(url, params=params, headers=headers) as response:
        if response.status == 200:
          result = await response.json()
          if request_id:
            return request_id, result
          return result
        elif response.status == 403:
          print(f'Retry for: {url}')
          headers['Authorization'] = get_token()
        raise aiohttp.ClientResponseError(
          response.request_info,
          response.history,
          status=response.status,
          message=response.reason,
          headers=response.headers
        )

async def fetch_workflow_data():
  def append_workflow_runs(workflow, runs):
    for run in runs:
      # Getting rid of all runs with a "skipped" status to display
      # only actual runs
      if run['conclusion'] != 'skipped':
        status = ''
        if run['status'] == 'completed':
          status = run['conclusion']
        elif run['status'] != 'cancelled':
          status = run['status']
        workflow.runs.append((int(run['id']), status, run['html_url']))

  url = "https://api.github.com/repos/apache/beam/actions/workflows"
  headers = {'Authorization': get_token()}
  page = 1
  number_of_entries_per_page = 100 # The number of results per page (max 100)
  params =\
    {'branch': 'master', 'page': page, 'per_page': number_of_entries_per_page}
  concurrent_requests = 30 # Number of requests to send simultaneously
  semaphore = asyncio.Semaphore(concurrent_requests)

  print("Start fetching recent workflow runs")
  workflow_tasks = []
  response = await fetch(url, semaphore, params, headers)
  pages_to_fetch =\
    math.ceil(response['total_count'] / number_of_entries_per_page)
  while pages_to_fetch >= page:
    params = {
      'branch': 'master',
      'page': page,
      'per_page': number_of_entries_per_page
    }
    workflow_tasks.append(fetch(url, semaphore, params, headers))
    page += 1

  workflow_run_tasks = []
  for completed_task in asyncio.as_completed(workflow_tasks):
    response = await completed_task
    workflows = response.get('workflows', [])
    for workflow in workflows:
      runs_url = f"{url}/{workflow['id']}/runs"
      page = 1
      pages_to_fetch = math.ceil(
        int(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH) / number_of_entries_per_page
      )
      while pages_to_fetch >= page:
        params = {
          'branch': 'master',
          'page': page,
          'per_page': number_of_entries_per_page,
          'exclude_pull_requests': 'true'
        }
        workflow_run_tasks.append(fetch(runs_url, semaphore, params, headers))
        page += 1
  print("Successfully fetched workflow runs")

  print("Start fetching workflow runs details")
  workflows = {}
  workflow_ids_to_fetch_extra_runs = {}
  for completed_task in asyncio.as_completed(workflow_run_tasks):
    response = await completed_task
    workflow_runs = response.get('workflow_runs')
    if workflow_runs:
      workflow_id = workflow_runs[0]['workflow_id']
      workflow = workflows.get(workflow_id)
      if not workflow:
        workflow_name = workflow_runs[0]['name']
        workflow_path = workflow_runs[0]['path']
        result = re.search(r'(workflows\/.*)$', workflow_path)
        if result:
          workflow_path = result.group(1)
        workflow = Workflow(workflow_id, workflow_name, workflow_path)

      append_workflow_runs(workflow, workflow_runs)
      workflows[workflow_id] = workflow
      if len(workflow.runs) < int(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH):
        workflow_ids_to_fetch_extra_runs[workflow_id] = workflow_id
      else:
        workflow_ids_to_fetch_extra_runs.pop(workflow_id, None)
      print(f"Successfully fetched details for: {workflow.filename}")
  
  page = math.ceil(
    int(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH) / number_of_entries_per_page
  ) + 1
  # Fetch extra workflow runs if the specified number of runs is not reached
  while workflow_ids_to_fetch_extra_runs:
    extra_workflow_runs_tasks = []
    for workflow_id in list(workflow_ids_to_fetch_extra_runs.values()):
      runs_url = f"{url}/{workflow_id}/runs"
      params = {
        'branch': 'master',
        'page': page,
        'per_page': number_of_entries_per_page,
        'exclude_pull_requests': 'true'
      }
      extra_workflow_runs_tasks.append(fetch(runs_url, semaphore, params, headers, workflow_id))
    for completed_task in asyncio.as_completed(extra_workflow_runs_tasks):
      workflow_id, response = await completed_task
      workflow = workflows[workflow_id]
      print(f"Fetching extra workflow runs for: {workflow.filename}")
      workflow_runs = response.get('workflow_runs')
      if workflow_runs:
        append_workflow_runs(workflow, workflow_runs)
      else:
        number_of_runs_to_add =\
          int(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH) - len(workflow.runs)
        workflow.runs.extend([(0, 'None', 'None')] * number_of_runs_to_add)
      if len(workflow.runs) >= int(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH):
          workflow_ids_to_fetch_extra_runs.pop(workflow_id, None)
      print(f"Successfully fetched extra workflow runs for: {workflow.filename}")
    page += 1
  print("Successfully fetched workflow runs details")

  for workflow in list(workflows.values()):
    runs = sorted(workflow.runs, key=lambda r: r[0], reverse=True)
    workflow.runs = runs[:int(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH)]

  return list(workflows.values())

def database_operations(connection, workflows):
  # Create the table and update it with the latest workflow runs
  if not workflows:
    return
  cursor = connection.cursor()
  workflows_table_name = "github_workflows"
  cursor.execute(f"DROP TABLE IF EXISTS {workflows_table_name};")
  create_table_query = f"""
  CREATE TABLE IF NOT EXISTS {workflows_table_name} (
    workflow_id integer NOT NULL PRIMARY KEY,
    job_name text NOT NULL,
    job_yml_filename text NOT NULL,
    dashboard_category text NOT NULL"""
  for i in range(int(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH)):
    create_table_query += f""",
    run{i+1} text,
    run{i+1}Id text"""
  create_table_query += ")\n"
  cursor.execute(create_table_query)
  insert_query = f"INSERT INTO {workflows_table_name} VALUES "
  for workflow in workflows:
    category = get_dashboard_category(workflow.name)
    row_insert =\
      f"(\'{workflow.id}\',\'{workflow.name}\',\'{workflow.filename}\',\'{category}\'"
    for _, status, url in workflow.runs:
      row_insert += f",\'{status}\',\'{url}\'"
    insert_query += f"{row_insert}),"
  insert_query = insert_query[:-1] + ";"
  print(insert_query)
  cursor.execute(insert_query)
  cursor.close()
  connection.commit()
  connection.close()

if __name__ == '__main__':
  asyncio.run(github_workflows_dashboard_sync())
