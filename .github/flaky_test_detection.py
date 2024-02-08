import os
import re
import json
import requests
from github import Github
from github import Auth

GIT_ORG = "volatilemolotov"
GRAFANA_URL = "https://tempgrafana.volatilemolotov.com"
GIT_REPO = "beam"
ALERT_NAME = "flaky_test"
READ_ONLY = os.environ.get("READ_ONLY", "false")


class Alert:
    def __init__(self, workflow_id, workflow_url, workflow_name, workflow_file_name):
        self.workflow_id = workflow_id
        self.workflow_url = workflow_url
        self.workflow_name = workflow_name
        self.workflow_file_name = workflow_file_name


def get_existing_issues(r):
    open_issues = r.get_issues(state="open", labels=["flaky_test"])
    existing_label_ids = []
    for issue in open_issues:
        for label in issue.get_labels():
            label_id = re.search(r"\d+", str(label.name))
            if label_id is not None:
                label_id = label_id.group()
                existing_label_ids.append(label_id)

    return existing_label_ids


def create_issue(r, alert):
    # REMOVE: debug
    GIT_ORG = "apache"
    failing_runs_url = f"https://github.com/{GIT_ORG}/{GIT_REPO}/actions/{alert.workflow_file_name}?query=is%3Afailure+branch%3Amaster"
    title_string = f"{alert.workflow_name} is failing"
    body_string = f"""It seems that the {alert.workflow_name }is failing
    Please visit {failing_runs_url} to see the logs"""
    labels_list = ["flaky_test", f"workflow id: {alert.workflow_id}"]
    print("-" * 10)
    print(f"Title: {title_string}")
    print(f"Body: {body_string}")
    print(f"Labels: {labels_list}")
    print("-" * 10)

    if READ_ONLY == "true":
        print("READ_ONLY is true, not creating issue")
    else:
        r.create_issue(title=title_string, labels=labels_list, body=body_string)


def get_grafana_alerts():
    alerts = []
    jsonAlerts = requests.get(
        f"{GRAFANA_URL}/api/alertmanager/grafana/api/v2/alerts?active=true&filter=alertname%3D{ALERT_NAME}"
    ).json()
    for alert in jsonAlerts:
        alerts.append(
            Alert(
                alert["labels"]["workflow_id"],
                alert["labels"]["workflow_url"],
                alert["labels"]["job_name"],
                alert["labels"]["job_yml_filename"],
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

    existing_label_ids = get_existing_issues(repo)
    for alert in alerts:
        if alert.workflow_id in existing_label_ids:
            print("issue already exists, skipping")
            continue
        create_issue(repo, alert)
        existing_label_ids.append(alert.workflow_id)

    g.close()


if __name__ == "__main__":
    main()
