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
        endpoint = f"search/issues/?q=is:issue+repo:{self.github_repo}+in:title+{title}+is:open"
        response = self._make_github_request("GET", endpoint)
        issues = response.json().get('items', [])
        return [GitHubIssue(**issue) for issue in issues]

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
        return GitHubIssue(**response.json())

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
        if open_issues:
            self.logger.info(f"Issue with title '{title}' already exists: #{open_issues[0].number}")
            announcement += f"\n\nRelated GitHub Issue: {open_issues[0].html_url}"

            if open_issues[0].body != body:
                self.logger.info(f"Updating body of issue #{open_issues[0].number}")
                self.update_issue_body(open_issues[0].number, body)
            else:
                self.logger.info(f"No changes detected for issue #{open_issues[0].number}")
            self._send_email(title, announcement, recipient)
        else:
            new_issue = self.create_issue(title, body)
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

        print("\nSimulating GitHub issue creation...")
        print(f"Title: {title}")
        print(f"Body: {body}")
