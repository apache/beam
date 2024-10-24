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

import os
import re
import requests
from datetime import datetime
from github import Github
from github import Auth


ALERT_NAME = "flaky_test"
GIT_ORG = "apache"
GRAFANA_URL = "http://metrics.beam.apache.org"
GRAFANA_PANEL_IDS = {
    "core_infra": 1,
    "important_signals": 2,
    "core_java": 3,
    "dataflow_java": 4,
    "runners_java": 5,
    "load_perf_java": 6,
    "core_python": 7,
    "runners_python": 8,
    "load_perf_python": 9,
    "go": 10,
    "misc": 11,
}
READ_ONLY = os.environ.get("READ_ONLY", "true")


class Alert:
    def __init__(
        self,
        workflow_id,
        workflow_url,
        workflow_name,
        workflow_filename,
        workflow_threshold,
        workflow_dashboard_category,
        workflow_retrieved_at,
    ):
        self.workflow_id = workflow_id
        self.workflow_url = workflow_url
        self.workflow_name = workflow_name
        self.workflow_filename = workflow_filename
        self.workflow_threshold = round(float(workflow_threshold), 2)
        self.workflow_dashboard_category = workflow_dashboard_category
        self.workflow_retrieved_at = workflow_retrieved_at


def get_workflow_issues(issues):
    workflows = {}
    for issue in issues:
        for label in issue.get_labels():
            match = re.search(r"workflow_id:\s*(\d+)", str(label.name))
            if match:
                workflow_id = match.group(1)
                workflows[workflow_id] = issue

    return workflows


def create_github_issue(repo, alert):
    github_workflow_failing_runs_url = f"https://github.com/{GIT_ORG}/beam/actions/{alert.workflow_filename}?query=is%3Afailure+branch%3Amaster"
    title = f"The {alert.workflow_name} job is flaky"
    body = f"The {alert.workflow_name } is failing over {int(alert.workflow_threshold * 100)}% of the time.\n" \
           f"Please visit {github_workflow_failing_runs_url} to see all failed workflow runs.\n"
    if GRAFANA_PANEL_IDS.get(alert.workflow_dashboard_category):
        grafana_workflow_runs_url = f"{GRAFANA_URL}/d/CTYdoxP4z/ga-post-commits-status?orgId=1" \
                           f"&viewPanel={GRAFANA_PANEL_IDS[alert.workflow_dashboard_category]}" \
                           f"&var-Workflow={alert.workflow_name.replace(' ', '%20')}"
        body += f"See also Grafana statistics: {grafana_workflow_runs_url}"
    labels = ["flaky_test", f"workflow_id: {alert.workflow_id}", "bug", "P1"]
    print("___")
    print(f"Title: {title}")
    print(f"Body: {body}")
    print(f"Labels: {labels}")
    print("___")

    if READ_ONLY == "true":
        print("READ_ONLY is true, not creating issue")
    else:
        repo.create_issue(title=title, body=body, labels=labels)


def get_grafana_alerts():
    url = f"{GRAFANA_URL}/api/alertmanager/grafana/api/v2/alerts?active=true&filter=alertname%3D{ALERT_NAME}"
    response = requests.get(url)
    if response.status_code != 200:
        raise RuntimeError(
            "Request to %s failed with status %d: %s"
            % (url, response.status_code, response.text)
        )
    alerts = []
    if response.text:
        for alert in response.json():
            alerts.append(
                Alert(
                    alert["labels"]["workflow_id"],
                    alert["labels"]["workflow_url"],
                    alert["labels"]["workflow_name"],
                    alert["labels"]["workflow_filename"],
                    alert["labels"]["workflow_threshold"],
                    alert["labels"]["dashboard_category"],
                    datetime.fromisoformat(alert["labels"]["workflow_retrieved_at"]),
                )
            )
    return alerts


def main():
    if "GITHUB_TOKEN" not in os.environ:
        print("Please set the GITHUB_TOKEN environment variable.")
        return

    token = os.environ["GITHUB_TOKEN"]
    auth = Auth.Token(token)
    g = Github(auth=auth)
    repo = g.get_repo(f"{GIT_ORG}/beam")

    alerts = get_grafana_alerts()
    open_issues = repo.get_issues(state="open", labels=["flaky_test"])
    closed_issues = repo.get_issues(state="closed", labels=["flaky_test"])
    workflow_open_issues = get_workflow_issues(open_issues)
    workflow_closed_issues = get_workflow_issues(closed_issues)
    for alert in alerts:
        if alert.workflow_id in workflow_closed_issues.keys():
            issue = workflow_closed_issues[alert.workflow_id]
            if READ_ONLY == "true":
                print("READ_ONLY is true, not reopening issue")
            elif issue.closed_at > alert.workflow_retrieved_at:
                print(f"The issue for the workflow {alert.workflow_id} has been closed, skipping")
            else:
                issue.edit(state="open")
                issue.create_comment(body="Reopening since the workflow is still flaky")
                print(f"The issue for the workflow {alert.workflow_id} has been reopened")
        elif alert.workflow_id not in workflow_open_issues.keys():
            create_github_issue(repo, alert)
        else:
            print(f"The issue for the workflow {alert.workflow_id} is already open, skipping")

    g.close()


if __name__ == "__main__":
    main()
