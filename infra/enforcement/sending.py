# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import requests
import logging
import smtplib, ssl
from typing import List, Optional
from dataclasses import dataclass

@dataclass
class GitHubIssue:
    """
    Represents a GitHub issue.
    """
    number: int
    title: str
    body: str
    state: str
    html_url: str
    created_at: str
    updated_at: str

class SendingClient:
    """
    Sends notifications about GitHub issues.
    """
    def __init__(self, logger: logging.Logger, github_token: str, github_repo: str,
                 smtp_server: str, smtp_port: int, email: str, password: str):

        required_keys = [github_token, github_repo, smtp_server, smtp_port, email, password]

        if not all(required_keys):
            raise ValueError("All parameters must be provided.")

        self.github_repo = github_repo
        self.headers = {
            "Authorization": f"Bearer {github_token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "Accept": "application/vnd.github+json"
        }

        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.email = email
        self.password = password

        self.logger = logger
        self.github_api_url = "https://api.github.com"

    def _make_github_request(self, method: str, endpoint: str, json: Optional[dict] = None) -> requests.Response:
        """
        Makes a request to the GitHub API.

        Args:
            method (str): The HTTP method to use (e.g., "GET", "POST", "PATCH").
            endpoint (str): The API endpoint to call.
            json (Optional[dict]): The JSON payload to send with the request.

        Returns:
            requests.Response: The response from the API.
        """
        url = f"{self.github_api_url}/{endpoint}"
        response = requests.request(method, url, headers=self.headers, json=json)
        
        if not response.ok:
            self.logger.error(f"Failed GitHub API request to {endpoint}: {response.status_code} - {response.text}")
            response.raise_for_status()
            
        return response

    def _send_email(self, title: str, body: str, recipient: str) -> None:
        """
        Sends an email notification.

        Args:
            title (str): The title of the email.
            body (str): The body content of the email.
            recipient (str): The email address of the recipient.
        """
        message = f"Subject: {title}\n\n{body}"
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, context=context) as server:
            server.login(self.email, self.password)
            server.sendmail(self.email, recipient, message)

    def _get_open_issues(self, title: str) -> List[GitHubIssue]:
        """
        Retrieves the number of open GitHub issues with a given title.

        Args:
            title (str): The title of the GitHub issue.
        """
        endpoint = f"search/issues?q=is:issue+repo:{self.github_repo}+in:title+{title}+is:open"
        response = self._make_github_request("GET", endpoint)
        issues = response.json().get('items', [])
        parsed_issues = []
        for issue in issues:
            parsed_issues.append(GitHubIssue(
                number=issue.get("number"),
                title=issue.get("title"),
                body=issue.get("body"),
                state=issue.get("state"),
                html_url=issue.get("html_url"),
                created_at=issue.get("created_at"),
                updated_at=issue.get("updated_at")
            ))
        return parsed_issues

    def create_issue(self, title: str, body: str) -> GitHubIssue:
        """
        Creates a GitHub issue in the specified repository.

        Args:
            title (str): The title of the GitHub issue.
            body (str): The body content of the GitHub issue.
        """
        endpoint = f"repos/{self.github_repo}/issues"
        payload = {"title": title, "body": body}
        response = self._make_github_request("POST", endpoint, json=payload)
        self.logger.info(f"Successfully created GitHub issue: {title}")
        data = response.json()
        return GitHubIssue(
            number=data.get("number"),
            title=data.get("title"),
            body=data.get("body"),
            state=data.get("state"),
            html_url=data.get("html_url"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at")
        )

    def update_issue_body(self, issue_number: int, new_body: str) -> None:
        """
        Updates the body of a GitHub issue in the specified repository.

        Args:
            issue_number (int): The number of the GitHub issue to update.
            new_body (str): The new body content for the GitHub issue.
        """
        endpoint = f"repos/{self.github_repo}/issues/{issue_number}"
        payload = {"body": new_body}
        self._make_github_request("PATCH", endpoint, json=payload)
        self.logger.info(f"Successfully updated body on GitHub issue: #{issue_number}")

    def create_issue_comment(self, issue_number: int, comment_body: str) -> None:
        """
        Adds a new comment to an existing GitHub issue in the specified repository.

        Args:
            issue_number (int): The number of the GitHub issue to comment on.
            comment_body (str): The content of the comment to add to the GitHub issue.
        """
        endpoint = f"repos/{self.github_repo}/issues/{issue_number}/comments"
        payload = {"body": comment_body}
        self._make_github_request("POST", endpoint, json=payload)
        self.logger.info(f"Successfully added comment to GitHub issue: #{issue_number}")

    def report_unmanaged_keys(self, project_id: str, compilance_issues: List[str]) -> None:
        """
        Report compliance issues regarding unmanaged keys into a single GitHub issue.
        Creates a new issue if none exists. If it exists, updates the body with the newest
        report and moves the previous content into a collapsed history section.

        Args:
            project_id (str): The ID of the project associated with the unmanaged keys.
            compilance_issues (List[str]): A list of compliance issues related to the unmanaged keys.
        """
        if not compilance_issues:
            self.logger.info("No compliance issues to report to Github.")
            return

        issue_title = "[SECURITY] Action Required: Unmanaged Service Account Keys Detected"
        #markdown body
        timestamp = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        new_report = f"### Unmanaged Keys Audit Report ({timestamp})\n"
        new_report += f"The following unauthorized or unmanaged keys were detected in `{project_id}`:\n\n"

        for issue_text in compilance_issues:
            new_report += f"- {issue_text}\n"

        new_report += "\n*Please investigate and revoke these keys if they are not part of the official rotation system.*"
        open_issues = self._get_open_issues(issue_title)

        if open_issues:
            target_issue = open_issues[0]
            self.logger.info(f"Appending report and archiving history to existing security issue #{target_issue.number}")

            old_body = target_issue.body or ""
            history_marker = "### History\n<details>\n<summary>Click to expand</summary>\n\n"

            if history_marker in old_body:
                # If history already exists, append the new report to it
                headed = old_body.split(history_marker)
                last_report = headed[0].strip()
                old_history = headed[1].replace("</details>", "").strip()

                combined_history = f"{last_report}\n\n---\n\n{old_history}"
            else:
                combined_history = old_body.strip()

            final_body = f"{new_report}\n\n{history_marker}{combined_history}\n</details>"
            self.update_issue_body(target_issue.number, final_body)
        else:
            self.logger.info("Creating new security issue for unmanaged keys report.")
            new_issue = self.create_issue(issue_title, new_report)
            self.logger.info(f"Created new security issue : {new_issue.html_url}.")

    def resolve_unmanaged_keys(self) -> None:
        """
        Finds any open security issues regarding rogue keys and automatically closes them
        if the infrastructure is now healthy.
        """
        issue_title = "[SECURITY] Action Required: Unmanaged Service Account Keys Detected"
        open_issues = self._get_open_issues(issue_title)
        if open_issues:
            target_issue = open_issues[0]
            self.logger.info(f"All rogue keys resolved! Auto-closing issue #{target_issue.number}.")
            self.create_issue_comment(target_issue.number, "All previously reported unmanaged keys have been resolved. Closing this issue.")
            endpoint = f"repos/{self.github_repo}/issues/{target_issue.number}"
            payload = {"state": "closed"}
            self._make_github_request("PATCH", endpoint, json=payload)


    def create_announcement(self, title: str, body: str, recipient: str, announcement: str) -> None:
        """
        This method sends an email with an announcement. The email will point to a GitHub issue.

        Creates a GitHub issue in the specified repository if it doesn't already exist.
        If multiple open versions exist, the most recent one will be updated.

        Args:
            title (str): The title of the GitHub issue.
            body (str): The body content of the GitHub issue.
            recipient (str): The email address of the recipient.
            announcement (str): The announcement message to include in the email.
        """
        open_issues = self._get_open_issues(title)
        open_issues.sort(key=lambda x: x.updated_at, reverse=True)

        timestamp = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        new_report = f"### Compliance Audit Report ({timestamp})\n{body}"

        if open_issues:
            target_issue = open_issues[0]
            self.logger.info(f"Issue with title '{title}' already exists: #{target_issue.number}")
            announcement += f"\n\nRelated GitHub Issue: {target_issue.html_url}"

            old_body = target_issue.body or ""
            history_marker = "### History\n<details>\n<summary>Click to expand</summary>\n\n"

            if history_marker in old_body:
                # If history already exists, append the new report to it
                headed = old_body.split(history_marker)
                last_report = headed[0].strip()
                old_history = headed[1].replace("</details>", "").strip()
                combined_history = f"{last_report}\n\n---\n\n{old_history}"
            else:
                # First time updating, turn the entire old body into history
                combined_history = old_body.strip()

            final_body = f"{new_report}\n\n{history_marker}{combined_history}\n</details>"

            self.logger.info(f"Appending report and archiving history to existing issue #{target_issue.number}")
            self.update_issue_body(target_issue.number, final_body)
            self._send_email(title, announcement, recipient)
        else:
            self.logger.info(f"Creating new compliance issue for: {title}")
            new_issue = self.create_issue(title, new_report)
            announcement += f"\n\nRelated GitHub Issue: {new_issue.html_url}"
            self._send_email(title, announcement, recipient)

    def print_announcement(self, title: str, body: str, recipient: str, announcement: str) -> None:
        """
        This method prints the data instead of sending the email or creating an issue.
        This is used for testing.
        """
        self.logger.info("Printing announcement...")
        print(f"Simulating email sending...")
        print(f"Recipient: {recipient}")
        print(f"Announcement: {announcement}")

        timestamp = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        print("\n" + "="*60)
        print("SIMULATING GITHUB GENERAL ISSUE CREATION/UPDATE")
        print("="*60)
        print(f"Title: {title}\n")
        print(f"Body:\n### Compliance Audit Report ({timestamp})")
        print(body)
        print("### History\n<details>\n<summary>Click to expand</summary>\n\n[... Previous reports would be collapsed here ...]\n</details>")
        print("="*60 + "\n")