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
from github import Github
from github import Auth


GIT_ORG = "apache"
GIT_REPO = "beam"
GRAFANA_URL = "https://metrics.beam.apache.org"
ALERT_NAME = "flaky_test"
READ_ONLY = os.environ.get("READ_ONLY", "false")


class Alert:
    def __init__(
        self,
        workflow_id,
        workflow_url,
        workflow_name,
        workflow_filename,
        workflow_threshold,
    ):
        self.workflow_id = workflow_id
        self.workflow_url = workflow_url
        self.workflow_name = workflow_name
        self.workflow_file_name = workflow_filename
        self.workflow_threshold = round(float(workflow_threshold), 2)


def extract_workflow_id_from_issue_label(issues):
    label_ids = []
    for issue in issues:
        for label in issue.get_labels():
            match = re.search(r"workflow_id:\s*(\d+)", str(label.name))
            if match:
                label_id = match.group(1)
                label_ids.append(label_id)

    return label_ids


def create_github_issue(repo, alert):
    failing_runs_url = f"https://github.com/{GIT_ORG}/beam/actions/{alert.workflow_file_name}?query=is%3Afailure+branch%3Amaster"
    title = f"The {alert.workflow_name} job is flaky"
    body = f"The {alert.workflow_name } is failing over {int(alert.workflow_threshold * 100)}% of the time \nPlease visit {failing_runs_url} to see the logs."
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
    repo = g.get_repo(f"{GIT_ORG}/{GIT_REPO}")

    alerts = get_grafana_alerts()
    open_issues = repo.get_issues(state="open", labels=["flaky_test"])
    workflow_ids = extract_workflow_id_from_issue_label(open_issues)
    for alert in alerts:
        if alert.workflow_id not in workflow_ids:
            create_github_issue(repo, alert)
            workflow_ids.append(alert.workflow_id)
        else:
            print("Issue already exists, skipping")

    g.close()


if __name__ == "__main__":
    main()
