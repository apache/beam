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

"""
This module queries GitHub API to collect Beam-related workflows metrics and
put them in PostgreSQL.
This script is running every 3 hours as a cloud function
"github_actions_workflows_dashboard_sync" in apache-beam-testing project:
https://console.cloud.google.com/functions/details/us-central1/github_actions_workflows_dashboard_sync?env=gen1&project=apache-beam-testing
This cloud function is triggered by a pubsub topic:
https://console.cloud.google.com/cloudpubsub/topic/detail/github_actions_workflows_sync?project=apache-beam-testing
Cron Job:
https://console.cloud.google.com/cloudscheduler/jobs/edit/us-central1/github_actions_workflows_dashboard_sync?project=apache-beam-testing
"""

import asyncio
import aiohttp
import backoff
import math
import os
import sys
import time
import re
import psycopg2
from psycopg2 import extras
from ruamel.yaml import YAML
from github import GithubIntegration

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_DBNAME"]
DB_USER_NAME = os.environ["DB_DBUSERNAME"]
DB_PASSWORD = os.environ["DB_DBPWD"]
GH_APP_ID = os.environ["GH_APP_ID"]
GH_APP_INSTALLATION_ID = os.environ["GH_APP_INSTALLATION_ID"]
GH_PEM_KEY = os.environ["GH_PEM_KEY"]
GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH = os.environ["GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH"]
GIT_REPO = "beam"
GIT_PATH = ".github/workflows"
GIT_FILESYSTEM_PATH = "/tmp/git"


class Workflow:
    def __init__(self, id, name, filename, url, category=None, threshold=0.5):
        self.id = id
        self.name = name
        self.filename = filename
        self.url = url
        self.runs = []
        self.category = category
        self.threshold = threshold


class WorkflowRun:
    def __init__(self, id, status, url, workflow_id, started_at):
        self.id = id
        self.status = status
        self.url = url
        self.workflow_id = workflow_id
        self.started_at = started_at


def clone_git_beam_repo(dest_path):
    filesystem_path = "/tmp/git"
    if not os.path.exists(filesystem_path):
        os.mkdir(filesystem_path)
    os.chdir(filesystem_path)
    os.system(f"git clone --filter=blob:none --sparse https://github.com/apache/beam")
    os.chdir("beam")
    os.system("git sparse-checkout init --cone")
    os.system(f"git sparse-checkout  set {dest_path}")
    os.chdir("/workspace/")


def get_yaml(file):
    yaml_file = YAML(typ="safe")
    if not os.path.exists(file):
        raise ValueError(f"Yaml file does not exist: {file}")
    with open(file) as f:
        return yaml_file.load(f)


def enhance_workflow(workflow):
    config = get_yaml("config.yaml")
    for category in config["categories"]:
        if workflow.name in category["tests"]:
            workflow.category = category["name"]
            if "groupThreshold" in category:
                workflow.threshold = category["groupThreshold"]
            break

    if not workflow.category:
        print(f"No category found for workflow: {workflow.name}")
        print("Falling back to rules based assignment")

        workflow_name = workflow.name.lower()
        if "java" in workflow_name:
            if "dataflow" in workflow_name:
                workflow.category = "dataflow_java"
            elif "spark" in workflow_name or "flink" in workflow_name:
                workflow.category = "runners_java"
            elif "performancetest" in workflow_name or "loadtest" in workflow_name:
                workflow.category = "load_perf_java"
            else:
                workflow.category = "core_java"
        elif "python" in workflow_name:
            if (
                "dataflow" in workflow_name
                or "spark" in workflow_name
                or "flink" in workflow_name
            ):
                workflow.category = "runners_python"
            elif "performancetest" in workflow_name or "loadtest" in workflow_name:
                workflow.category = "load_perf_python"
            else:
                workflow.category = "core_python"
        elif "go" in workflow_name:
            workflow.category = "go"
        else:
            workflow.category = "misc"

    workflow_filename = workflow.filename.replace("workflows/", "")
    try:
        workflow_yaml = get_yaml(
            f"{GIT_FILESYSTEM_PATH}/{GIT_REPO}/{GIT_PATH}/{workflow_filename}"
        )
        if "env" in workflow_yaml:
            if "ALERT_THRESHOLD" in workflow_yaml["env"]:
                workflow.threshold = workflow_yaml["env"]["ALERT_THRESHOLD"]
    except ValueError:
        print(f"No yaml file found for workflow: {workflow.name}")


def github_workflows_dashboard_sync(request):
    # Entry point for cloud function, don't change signature
    return asyncio.run(sync_workflow_runs())


async def sync_workflow_runs():
    print("Started")
    print("Updating table with recent workflow runs")

    if (
        not GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH
        or not GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH.isdigit()
    ):
        raise ValueError(
            "The number of workflow runs to fetch is not specified or not an integer"
        )

    clone_git_beam_repo(GIT_PATH)
    workflows = await fetch_workflow_runs()
    for workflow in workflows:
        enhance_workflow(workflow)

    save_workflows(workflows)

    print("Done")
    return "Completed"


def init_db_connection():
    """Init connection with the Database"""
    connection = None
    max_retries = 3
    i = 0
    while connection is None and i < max_retries:
        try:
            connection = psycopg2.connect(
                f"dbname='{DB_NAME}' user='{DB_USER_NAME}' host='{DB_HOST}'"
                f" port='{DB_PORT}' password='{DB_PASSWORD}'"
            )
        except Exception as e:
            print("Failed to connect to DB; retrying in 1 minute")
            print(e)
            time.sleep(60)
            i = i + 1
            if i >= max_retries:
                print("Number of retries exceded ")
                sys.exit(1)
    return connection


def get_token():
    git_integration = GithubIntegration(GH_APP_ID, GH_PEM_KEY)
    token = git_integration.get_access_token(GH_APP_INSTALLATION_ID).token
    return f"Bearer {token}"


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
                    print(f"Retry for: {url}")
                    headers["Authorization"] = get_token()
                raise aiohttp.ClientResponseError(
                    response.request_info,
                    response.history,
                    status=response.status,
                    message=response.reason,
                    headers=response.headers,
                )


async def fetch_workflow_runs():
    def append_workflow_runs(workflow, runs):
        for run in runs:
            # Getting rid of all runs with a "skipped" status to display
            # only actual runs
            if run["conclusion"] != "skipped":
                status = ""
                if run["status"] == "completed":
                    status = run["conclusion"]
                elif run["status"] != "cancelled":
                    status = run["status"]
                workflow.runs.append(
                    WorkflowRun(
                        int(run["id"]),
                        status,
                        run["html_url"],
                        workflow.id,
                        run["run_started_at"],
                    )
                )

    url = "https://api.github.com/repos/apache/beam/actions/workflows"
    headers = {"Authorization": get_token()}
    page = 1
    number_of_entries_per_page = 100  # The number of results per page (max 100)
    params = {"branch": "master", "page": page, "per_page": number_of_entries_per_page}
    concurrent_requests = 30  # Number of requests to send simultaneously
    semaphore = asyncio.Semaphore(concurrent_requests)

    print("Start fetching recent workflow runs")
    workflow_tasks = []
    response = await fetch(url, semaphore, params, headers)
    pages_to_fetch = math.ceil(response["total_count"] / number_of_entries_per_page)
    while pages_to_fetch >= page:
        params = {
            "branch": "master",
            "page": page,
            "per_page": number_of_entries_per_page,
        }
        workflow_tasks.append(fetch(url, semaphore, params, headers))
        page += 1

    workflow_run_tasks = []
    for completed_task in asyncio.as_completed(workflow_tasks):
        response = await completed_task
        workflows = response.get("workflows", [])
        for workflow in workflows:
            runs_url = f"{url}/{workflow['id']}/runs"
            page = 1
            pages_to_fetch = math.ceil(
                int(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH) / number_of_entries_per_page
            )
            while pages_to_fetch >= page:
                params = {
                    "branch": "master",
                    "page": page,
                    "per_page": number_of_entries_per_page,
                    "exclude_pull_requests": "true",
                }
                workflow_run_tasks.append(fetch(runs_url, semaphore, params, headers))
                page += 1
    print("Successfully fetched workflow runs")

    print("Start fetching workflow runs details")
    workflows = {}
    workflow_ids_to_fetch_extra_runs = {}
    for completed_task in asyncio.as_completed(workflow_run_tasks):
        response = await completed_task
        workflow_runs = response.get("workflow_runs")
        if workflow_runs:
            workflow_id = workflow_runs[0]["workflow_id"]
            workflow = workflows.get(workflow_id)
            if not workflow:
                workflow_name = workflow_runs[0]["name"]
                workflow_path = workflow_runs[0]["path"]
                workflow_url = workflow_runs[0]["workflow_url"]
                result = re.search(r"(workflows\/.*)$", workflow_path)
                if result:
                    workflow_path = result.group(1)
                workflow = Workflow(
                    workflow_id, workflow_name, workflow_path, workflow_url
                )

            append_workflow_runs(workflow, workflow_runs)
            workflows[workflow_id] = workflow
            if len(workflow.runs) < int(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH):
                workflow_ids_to_fetch_extra_runs[workflow_id] = workflow_id
            else:
                workflow_ids_to_fetch_extra_runs.pop(workflow_id, None)
            print(f"Successfully fetched details for: {workflow.filename}")

    page = (
        math.ceil(int(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH) / number_of_entries_per_page)
        + 1
    )
    # Fetch extra workflow runs if the specified number of runs is not reached
    while workflow_ids_to_fetch_extra_runs:
        extra_workflow_runs_tasks = []
        for workflow_id in list(workflow_ids_to_fetch_extra_runs.values()):
            runs_url = f"{url}/{workflow_id}/runs"
            params = {
                "branch": "master",
                "page": page,
                "per_page": number_of_entries_per_page,
                "exclude_pull_requests": "true",
            }
            extra_workflow_runs_tasks.append(
                fetch(runs_url, semaphore, params, headers, workflow_id)
            )
        for completed_task in asyncio.as_completed(extra_workflow_runs_tasks):
            workflow_id, response = await completed_task
            workflow = workflows[workflow_id]
            print(f"Fetching extra workflow runs for: {workflow.filename}")
            workflow_runs = response.get("workflow_runs")
            if workflow_runs:
                append_workflow_runs(workflow, workflow_runs)
            else:
                number_of_runs_to_add = int(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH) - len(
                    workflow.runs
                )
                workflow.runs.extend(
                    [WorkflowRun(0, "None", "None", workflow.id, "None")]
                    * number_of_runs_to_add
                )
            if len(workflow.runs) >= int(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH):
                workflow_ids_to_fetch_extra_runs.pop(workflow_id, None)
            print(f"Successfully fetched extra workflow runs for: {workflow.filename}")
        page += 1
    print("Successfully fetched workflow runs details")

    for workflow in list(workflows.values()):
        runs = sorted(workflow.runs, key=lambda r: r.id, reverse=True)
        workflow.runs = runs[: int(GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH)]

    return list(workflows.values())


def save_workflows(workflows):
    connection = init_db_connection()
    # Create the table and update it with the latest workflow runs
    if not workflows:
        return
    cursor = connection.cursor()
    workflows_table_name = "github_workflows"
    workflow_runs_table_name = "github_workflow_runs"
    cursor.execute(f"DROP TABLE IF EXISTS {workflow_runs_table_name};")
    cursor.execute(f"DROP TABLE IF EXISTS {workflows_table_name};")
    create_workflows_table_query = f"""
    CREATE TABLE IF NOT EXISTS {workflows_table_name} (
      workflow_id integer NOT NULL PRIMARY KEY,
      name text NOT NULL,
      filename text NOT NULL,
      url text NOT NULL,
      dashboard_category text NOT NULL,
      threshold real NOT NULL)\n"""
    create_workflow_runs_table_query = f"""
    CREATE TABLE IF NOT EXISTS {workflow_runs_table_name} (
        run_id text NOT NULL PRIMARY KEY,
        run_number integer NOT NULL,
        status text NOT NULL,
        url text NOT NULL,
        workflow_id integer NOT NULL,
        started_at timestamp with time zone NOT NULL,
        CONSTRAINT fk_workflow FOREIGN KEY(workflow_id) REFERENCES {workflows_table_name}(workflow_id))\n"""
    cursor.execute(create_workflows_table_query)
    cursor.execute(create_workflow_runs_table_query)
    insert_workflows_query = f"""
    INSERT INTO {workflows_table_name} (workflow_id, name, filename, url,  dashboard_category, threshold)
    VALUES %s"""
    insert_workflow_runs_query = f"""
    INSERT INTO {workflow_runs_table_name} (run_id, run_number, status, url, workflow_id, started_at)
    VALUES %s"""
    insert_workflows = []
    insert_workflow_runs = []
    for workflow in workflows:
        insert_workflows.append(
            (
                workflow.id,
                workflow.name,
                workflow.filename,
                workflow.url,
                workflow.category,
                workflow.threshold,
            )
        )
        run_number = 1
        for run in workflow.runs:
            if run.id != 0:
                started_at = run.started_at.replace("T", " ")
                insert_workflow_runs.append(
                    (run.id, run_number, run.status, run.url, workflow.id, started_at)
                )
                run_number += 1
    psycopg2.extras.execute_values(cursor, insert_workflows_query, insert_workflows)
    psycopg2.extras.execute_values(
        cursor, insert_workflow_runs_query, insert_workflow_runs
    )
    cursor.close()
    connection.commit()
    connection.close()


if __name__ == "__main__":
    asyncio.run(github_workflows_dashboard_sync(None, None))
