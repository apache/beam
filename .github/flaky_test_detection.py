import os
import re
import requests
from github import Github
from github import Auth

# TODO: remove
GIT_ORG = "volatilemolotov"
GIT_REPO = "beam"
GRAFANA_URL = "https://tempgrafana.volatilemolotov.com"
#
ALERT_NAME = "flaky_test"
READ_ONLY = os.environ.get("READ_ONLY", "false")


class Alert:
    def __init__(self, id, url, name, file_name):
        self.workflow_id = id
        self.workflow_url = url
        self.workflow_name = name
        self.workflow_file_name = file_name


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
    body = f"The {alert.workflow_name } is constantly failing.\nPlease visit {failing_runs_url} to see the logs."
    labels = ["flaky_test", f"workflow_id: {alert.workflow_id}"]
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
            "Request to %s failed with status %d: %s" %
            (url, response.status_code, response.text))
    alerts = []
    if response.text:
        for alert in response.json():
            alerts.append(
                Alert(
                    alert["labels"]["workflow_id"],
                    alert["labels"]["url"],
                    alert["labels"]["name"],
                    alert["labels"]["filename"],
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
