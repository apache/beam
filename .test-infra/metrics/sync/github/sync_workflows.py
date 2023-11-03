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

# Maps workflows to dashboard category. Any workflows not in one of these lists
# will get auto-mapped to misc.
CORE_JAVA_TESTS = [
  'PreCommit SQL Java17',
  'PreCommit SQL Java11',
  'LoadTests Java GBK Smoke',
  'PreCommit Java Amazon-Web-Services IO Direct',
  'PreCommit Java Amqp IO Direct',
  'PreCommit Java Amazon-Web-Services2 IO Direct',
  'PreCommit Java',
  'PreCommit Java Cassandra IO Direct',
  'PreCommit Java Azure IO Direct',
  'PreCommit Java Cdap IO Direct',
  'PreCommit Java Clickhouse IO Direct',
  'PreCommit Java Csv IO Direct',
  'Java Tests',
  'PostCommit Java Avro Versions',
  'PreCommit Java Debezium IO Direct',
  'PreCommit Java File-schema-transform IO Direct',
  'PostCommit Java',
  'PreCommit Java GCP IO Direct',
  'PostCommit Java BigQueryEarlyRollout',
  'PreCommit Java Google-ads IO Direct',
  'PreCommit Java HBase IO Direct',
  'PreCommit Java ElasticSearch IO Direct',
  'PreCommit Java HCatalog IO Direct',
  'PreCommit Java Hadoop IO Direct',
  'PreCommit Java IOs Direct',
  'PostCommit Java Hadoop Versions',
  'PreCommit Java Jms IO Direct',
  'PostCommit Java ValidatesRunner Direct JavaVersions',
  'PreCommit Java Kafka IO Direct',
  'PostCommit Java Examples Direct',
  'PreCommit Java JDBC IO Direct',
  'PostCommit Java ValidatesRunner Samza',
  'PreCommit Java Mqtt IO Direct',
  'PreCommit Java Kinesis IO Direct',
  'PreCommit Java MongoDb IO Direct',
  'PostCommit Java IO Performance Tests',
  'PreCommit Java Kudu IO Direct',
  'PostCommit Java InfluxDbIO Integration Test',
  'PostCommit Java Jpms Direct Java21',
  'PostCommit Java ValidatesRunner Twister2',
  'PreCommit Java Neo4j IO Direct',
  'PostCommit Java Jpms Direct Java11',
  'PostCommit Javadoc',
  'PostCommit Java Jpms Direct Java17',
  'PreCommit Java Pulsar IO Direct',
  'PostCommit Java ValidatesRunner ULR',
  'PreCommit Java Parquet IO Direct',
  'PreCommit Java Redis IO Direct',
  'Java JMH',
  'PreCommit Java RabbitMq IO Direct',
  'PreCommit Java RequestResponse IO Direct',
  'PostCommit Java Nexmark Direct',
  'PreCommit Java Splunk IO Direct',
  'PreCommit Java Thrift IO Direct',
  'PreCommit Java Snowflake IO Direct',
  'PreCommit Java Solr IO Direct',
  'PostCommit Java PVR Samza',
  'PreCommit Java Tika IO Direct',
  'PostCommit Java SingleStoreIO IT',
  'PostCommit Java Sickbay',
  'PostCommit Java ValidatesRunner Direct',
  'PreCommit Java SingleStore IO Direct',
  'PreCommit Java InfluxDb IO Direct',
  'PreCommit Spotless',
  'PreCommit Kotlin Examples'
]

DATAFLOW_JAVA_TESTS = [
  'PostCommit XVR GoUsingJava Dataflow',
  'PostCommit XVR PythonUsingJavaSQL Dataflow',
  'PostCommit XVR JavaUsingPython Dataflow',
  'PostCommit XVR PythonUsingJava Dataflow',
  'PreCommit Java Examples Dataflow Java11',
  'PreCommit Java Examples Dataflow Java17',
  'PreCommit Java Examples Dataflow Java21',
  'PreCommit Java Examples Dataflow',
  'PostCommit Java ValidatesRunner Dataflow',
  'PostCommit Java Dataflow V1',
  'PostCommit Java ValidatesRunner Dataflow Streaming',
  'PostCommit Java Dataflow V2',
  'PostCommit Java ValidatesRunner Dataflow V2',
  'PostCommit Java Examples Dataflow',
  'PostCommit Java Examples Dataflow ARM',
  'PostCommit Java ValidatesRunner Dataflow V2 Streaming',
  'PostCommit Java ValidatesRunner Dataflow JavaVersions',
  'PostCommit Java Examples Dataflow Java',
  'PostCommit Java Examples Dataflow V2 Java',
  'PostCommit Java Jpms Dataflow Java11',
  'PostCommit Java Jpms Dataflow Java17',
  'PostCommit Java Nexmark Dataflow',
  'PostCommit Java Nexmark Dataflow V2',
  'PostCommit Java Nexmark Dataflow V2 Java',
  'PostCommit Java Tpcds Dataflow',
  'PostCommit Java Examples Dataflow V2'
]

RUNNERS_JAVA_TESTS = [
  'PostCommit Java PVR Spark3 Streaming',
  'PostCommit Java ValidatesRunner Spark',
  'PostCommit Java Examples Spark',
  'PostCommit Java ValidatesRunner SparkStructuredStreaming',
  'PostCommit Java ValidatesRunner Spark Java11',
  'PostCommit Java PVR Spark Batch',
  'PreCommit Java Spark3 Versions',
  'PostCommit Java Tpcds Spark',
  'PostCommit Java Jpms Spark Java11',
  'PostCommit Java Nexmark Spark',
  'PostCommit Java Examples Flink',
  'PostCommit Java Tpcds Flink',
  'PostCommit Java PVR Flink Streaming',
  'PostCommit Java Jpms Flink Java11',
  'PreCommit Java PVR Flink Batch',
  'PostCommit Java Nexmark Flink',
  'PreCommit Java PVR Flink Docker',
  'PreCommit Java Flink Versions',
  'PostCommit Java ValidatesRunner Flink Java11',
  'PostCommit Java ValidatesRunner Flink'
]

LOAD_PERF_JAVA_TESTS = [
  'LoadTests Java CoGBK Dataflow Batch',
  'LoadTests Java CoGBK Dataflow V2 Streaming JavaVersions',
  'LoadTests Java CoGBK Dataflow Streaming',
  'LoadTests Java Combine Dataflow Batch',
  'LoadTests Java Combine Dataflow Streaming',
  'LoadTests Java CoGBK Dataflow V2 Batch JavaVersions',
  'LoadTests Java GBK Dataflow Batch',
  'LoadTests Java GBK Dataflow Streaming',
  'LoadTests Java GBK Dataflow V2 Batch Java11',
  'LoadTests Java GBK Dataflow V2 Streaming Java11',
  'LoadTests Java GBK Dataflow V2 Batch Java17',
  'LoadTests Java GBK Dataflow V2 Streaming Java17',
  'LoadTests Java ParDo Dataflow Streaming',
  'LoadTests Java ParDo Dataflow V2 Streaming JavaVersions',
  'LoadTests Java ParDo Dataflow V2 Batch JavaVersions',
  'LoadTests Java ParDo Dataflow Batch',
  'LoadTests Java ParDo SparkStructuredStreaming Batch',
  'LoadTests Java CoGBK SparkStructuredStreaming Batch',
  'LoadTests Java Combine SparkStructuredStreaming Batch',
  'LoadTests Java GBK SparkStructuredStreaming Batch',
  'PerformanceTests BigQueryIO Batch Java Avro',
  'PerformanceTests BigQueryIO Streaming Java',
  'PerformanceTests BigQueryIO Batch Java Json',
  'PerformanceTests SQLBigQueryIO Batch Java',
  'PerformanceTests XmlIOIT',
  'PostCommit XVR Samza',
  'PerformanceTests ManyFiles TextIOIT',
  'PerformanceTests XmlIOIT HDFS',
  'PerformanceTests ParquetIOIT',
  'PerformanceTests ParquetIOIT HDFS',
  'PerformanceTests AvroIOIT',
  'PerformanceTests ManyFiles TextIOIT HDFS',
  'PerformanceTests TFRecordIOIT',
  'PerformanceTests Cdap',
  'PerformanceTests TextIOIT',
  'PerformanceTests AvroIOIT HDFS',
  'PerformanceTests SingleStoreIO',
  'PerformanceTests SparkReceiver IO',
  'PerformanceTests Compressed TextIOIT',
  'PerformanceTests TextIOIT HDFS',
  'PerformanceTests Compressed TextIOIT HDFS',
  'PerformanceTests HadoopFormat',
  'PerformanceTests JDBC',
  'PerformanceTests Kafka IO'
]

CORE_PYTHON_TESTS = [
  'Python Dependency Tests',
  'PreCommit Python Dataframes',
  'PreCommit Python Examples',
  'PreCommit Python Integration',
  'PostCommit Python ValidatesRunner Samza',
  'LoadTests Python Smoke',
  'Update Python Depedencies',
  'PreCommit Python Runners',
  'PreCommit Python Transforms',
  'PostCommit Python Xlang Gcp Direct',
  'Build python source distribution and wheels',
  'Python tests',
  'PostCommit Sickbay Python',
  'PostCommit Python',
  'PostCommit Python Arm',
  'PostCommit Python Examples Direct',
  'PreCommit Portable Python',
  'PreCommit Python Coverage',
  'PreCommit Python Docker',
  'PreCommit Python',
  'PostCommit Python MongoDBIO IT',
  'PreCommit Python Docs',
  'PreCommit Python Formatter',
  'PostCommit Python Nexmark Direct',
  'PreCommit Python Lint'
]

RUNNERS_PYTHON_TESTS = [
  'PostCommit Python ValidatesRunner Dataflow',
  'Python ValidatesContainer Dataflow ARM',
  'PostCommit Python Xlang Gcp Dataflow',
  'PostCommit Python Xlang IO Dataflow',
  'PostCommit Python Examples Dataflow',
  'PostCommit Python ValidatesContainer Dataflow',
  'PostCommit Python ValidatesContainer Dataflow With RC',
  'PostCommit Python ValidatesRunner Spark',
  'PostCommit Python Examples Spark',
  'PostCommit Python ValidatesRunner Flink',
  'PreCommit Python PVR Flink',
  'PostCommit Python Examples Flink'
]

LOAD_PERF_PYTHON_TESTS = [
  'PerformanceTests xlang KafkaIO Python',
  'LoadTests Python FnApiRunner Microbenchmark',
  'PerformanceTests SpannerIO Write 2GB Python Batch',
  'PerformanceTests SpannerIO Read 2GB Python',
  'PerformanceTests BiqQueryIO Read Python',
  'PerformanceTests BiqQueryIO Write Python Batch',
  'PerformanceTests TextIOIT Python',
  'PerformanceTests WordCountIT PythonVersions',
  'Performance alerting tool on Python load/performance/benchmark tests.',
  'LoadTests Python SideInput Dataflow Batch',
  'LoadTests Python CoGBK Dataflow Batch',
  'LoadTests Python CoGBK Dataflow Streaming',
  'LoadTests Python Combine Dataflow Batch',
  'Inference Python Benchmarks Dataflow',
  'LoadTests Python Combine Dataflow Streaming',
  'LoadTests Python GBK Dataflow Batch',
  'LoadTests Python GBK Dataflow Streaming',
  'LoadTests Python GBK reiterate Dataflow Batch',
  'LoadTests Python GBK reiterate Dataflow Streaming',
  'LoadTests Python ParDo Dataflow Streaming',
  'CloudML Benchmarks Dataflow',
  'LoadTests Python ParDo Dataflow Batch',
  'LoadTests Python CoGBK Flink Batch',
  'LoadTests Python Combine Flink Batch',
  'LoadTests Python Combine Flink Streaming',
  'PerformanceTests PubsubIOIT Python Streaming',
  'LoadTests Python ParDo Flink Batch',
  'LoadTests Python ParDo Flink Streaming'
]

GO_TESTS = [
  'PerformanceTests MongoDBIO IT',
  'PreCommit Go',
  'PreCommit GoPortable',
  'PreCommit GoPrism',
  'PostCommit Go VR Samza',
  'Go tests',
  'PostCommit Go',
  'PostCommit Go Dataflow ARM',
  'LoadTests Go CoGBK Dataflow Batch',
  'LoadTests Go Combine Dataflow Batch',
  'LoadTests Go GBK Dataflow Batch',
  'LoadTests Go ParDo Dataflow Batch',
  'LoadTests Go SideInput Dataflow Batch',
  'PostCommit Go VR Spark',
  'PostCommit Go VR Flink',
  'LoadTests Go CoGBK Flink Batch',
  'LoadTests Go Combine Flink Batch',
  'LoadTests Go GBK Flink Batch',
  'LoadTests Go ParDo Flink Batch',
  'LoadTests Go SideInput Flink Batch'
]

CORE_INFRA_TESTS = [
  'Release Nightly Snapshot Python',
  'Rotate Metrics Cluster Credentials',
  'Community Metrics Prober',
  'Publish Docker Snapshots',
  'Clean Up GCP Resources',
  'Clean Up Prebuilt SDK Images',
  'Rotate IO-Datastores Cluster Credentials',
  'Release Nightly Snapshot',
  'Mark issue as triaged when assigned',
  'PostCommit BeamMetrics Publish',
  'PreCommit Community Metrics',
  'Beam Metrics Report',
  'Build and Version Runner Docker Image',
  'PreCommit GHA',
  'pr-bot-prs-needing-attention',
  'PreCommit RAT',
  'Assign or close an issue',
  'PostCommit Website Test',
  'PostCommit Website Publish',
  'PreCommit Website',
  'PreCommit Website Stage GCS',
  'Cleanup Dataproc Resources',
  'PreCommit Whitespace',
  'Publish Beam SDK Snapshots',
  'Cancel Stale Dataflow Jobs',
  'pr-bot-pr-updates',
  'pr-bot-new-prs'
]

MISC_TESTS = [
  'Tour of Beam Go integration tests',
  'Tour of Beam Go unittests',
  'Tour Of Beam Frontend Test',
  'PostCommit XVR Spark3',
  'TypeScript Tests',
  'Playground Frontend Test',
  'PostCommit PortableJar Flink',
  'PostCommit SQL',
  'Cancel',
  'PostCommit PortableJar Spark',
  'PreCommit Integration and Load Test Framework',
  'pr-bot-update-reviewers',
  'PostCommit TransformService Direct',
  'Cut Release Branch',
  'Generate issue report',
  'Dask Runner Tests',
  'PreCommit Typescript',
  'PostCommit XVR Direct',
  'Mark and close stale pull requests',
  'PostCommit XVR Flink',
  'IssueTagger',
  'Assign Milestone on issue close',
  'Local environment tests',
  'PreCommit SQL',
  'LabelPrs',
  'build_release_candidate'
]

class Workflow:
  def __init__(self, id, name, filename):
    self.id = id
    self.name = name
    self.filename = filename
    self.runs = []

def get_dashboard_category(workflow_name):
  # If you add or remove categories in this function, make sure to add or
  # remove the corresponding panels here:
  # https://github.com/apache/beam/blob/master/.test-infra/metrics/grafana/dashboards/GA-Post-Commits_status_dashboard.json

  if workflow_name in CORE_INFRA_TESTS:
    return 'core_infra'
  if workflow_name in CORE_JAVA_TESTS:
    return 'core_java'
  if workflow_name in DATAFLOW_JAVA_TESTS:
    return 'dataflow_java'
  if workflow_name in RUNNERS_JAVA_TESTS:
    return 'runners_java'
  if workflow_name in LOAD_PERF_JAVA_TESTS:
    return 'load_perf_java'
  if workflow_name in CORE_PYTHON_TESTS:
    return 'core_python'
  if workflow_name in RUNNERS_PYTHON_TESTS:
    return 'runners_python'
  if workflow_name in LOAD_PERF_PYTHON_TESTS:
    return 'load_perf_python'
  if workflow_name in GO_TESTS:
    return 'go'
  if workflow_name in MISC_TESTS:
    return 'misc'
  
  print(f'No category found for workflow: {workflow_name}')
  print('Falling back to rules based assignment')

  workflow_name = workflow_name.lower()
  if 'java' in workflow_name:
    if 'dataflow' in workflow_name:
      return 'dataflow_java'
    if 'spark' in workflow_name or 'flink' in workflow_name:
      return 'runners_java'
    if 'performancetest' in workflow_name or 'loadtest' in workflow_name:
      return 'load_perf_java'
    return 'core_java'
  elif 'python' in workflow_name:
    if 'dataflow' in workflow_name or 'spark' in workflow_name or 'flink' in workflow_name:
      return 'runners_python'
    if 'performancetest' in workflow_name or 'loadtest' in workflow_name:
      return 'load_perf_python'
    return 'core_python'
  elif 'go' in workflow_name:
    return 'go'

  return 'misc'

def github_workflows_dashboard_sync(data, context):
  # Entry point for cloud function, don't change signature
  return asyncio.run(sync_workflow_runs())

async def sync_workflow_runs():
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
  asyncio.run(github_workflows_dashboard_sync(None, None))
