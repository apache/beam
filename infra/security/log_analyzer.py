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

import json
import ssl
import yaml
import logging
import smtplib
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from google.cloud import logging_v2
from google.cloud import storage
from typing import List, Dict, Any
import argparse

REPORT_SUBJECT = "Weekly IAM Security Events Report"
REPORT_BODY_TEMPLATE = """
Hello Team,

Please find below the summary of IAM security events for the past week:

{event_summary}

Best Regards,
Automated GitHub Action
"""

@dataclass
class SinkCls:
    name: str
    description: str
    filter_methods: List[str]
    excluded_principals: List[str]

class LogAnalyzer():
    def __init__(self, project_id: str, gcp_bucket: str, logger: logging.Logger, sinks: List[SinkCls]):
        self.project_id = project_id
        self.bucket = gcp_bucket
        self.logger = logger
        self.sinks = sinks

    def _construct_filter(self, sink: SinkCls) -> str:
        """
        Constructs a filter string for a given sink.

        Args:
            sink (Sink): The sink object containing filter information.

        Returns:
            str: The constructed filter string.
        """

        method_filters = []
        for method in sink.filter_methods:
            method_filters.append(f'protoPayload.methodName="{method}"')

        exclusion_filters = []
        for principal in sink.excluded_principals:
            exclusion_filters.append(f'protoPayload.authenticationInfo.principalEmail != "{principal}"')

        if method_filters and exclusion_filters:
            filter_ = f"({' OR '.join(method_filters)}) AND ({' AND '.join(exclusion_filters)})"
        elif method_filters:
            filter_ = f"({' OR '.join(method_filters)})"
        elif exclusion_filters:
            filter_ = f"({' AND '.join(exclusion_filters)})"
        else:
            filter_ = ""

        return filter_

    def _create_log_sink(self, sink: SinkCls) -> None:
        """
        Creates a log sink in GCP if it doesn't already exist.
        If it already exists, it updates the sink with the new filter in case the filter has changed.

        Args:
            sink (Sink): The sink object to create.
        """
        logging_client = logging_v2.Client(project=self.project_id)
        filter_ = self._construct_filter(sink)
        destination = "storage.googleapis.com/{bucket}".format(bucket=self.bucket)

        sink_client = logging_client.sink(sink.name, filter_=filter_, destination=destination)

        if sink_client.exists():
            self.logger.debug(f"Sink {sink.name} already exists.")
            sink_client.reload()
            if sink_client.filter_ != filter_:
                sink_client.filter_ = filter_
                sink_client.update()
                self.logger.info(f"Updated sink {sink.name}'s filter.")
        else:
            sink_client.create()
            self.logger.info(f"Created sink {sink.name}.")
            # Reload the sink to get the writer_identity, this may take a few moments
            sink_client.reload()

        self._grant_bucket_permissions(sink_client)

        logging_client.close()

    def _grant_bucket_permissions(self, sink: logging_v2.Sink) -> None:
        """
        Grants a log sink's writer identity permissions to write to the bucket.
        """
        logging_client = logging_v2.Client(project=self.project_id)
        storage_client = storage.Client(project=self.project_id)

        sink.reload()    
        writer_identity = sink.writer_identity
        if not writer_identity:
            self.logger.warning(f"Could not retrieve writer identity for sink {sink.name}. "
                                f"Manual permission granting might be required.")
            return

        bucket = storage_client.get_bucket(self.bucket)
        policy = bucket.get_iam_policy(requested_policy_version=3)
        iam_role = "roles/storage.objectCreator"

        # Workaround for projects where the writer_identity is not a valid service account.
        if writer_identity == "serviceAccount:cloud-logs@system.gserviceaccount.com":
            member = "group:cloud-logs@google.com"
        else:
            member = f"serviceAccount:{writer_identity}"

        # Check if the policy is already configured
        if any(member in b.get("members", []) and b.get("role") == iam_role for b in policy.bindings):
            self.logger.debug(f"Sink {sink.name} already has the necessary permissions.")
            return

        policy.bindings.append({
            "role": iam_role,
            "members": {member}
        })

        bucket.set_iam_policy(policy)
        self.logger.info(f"Granted {iam_role} to {member} on bucket {self.bucket} for sink {sink.name}.")

    def initialize_sinks(self) -> None:
        for sink in self.sinks:
            self._create_log_sink(sink)
            self.logger.info(f"Initialized sink: {sink.name}")

    def get_event_logs(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Reads and retrieves log events from the specified time range from the GCP Cloud Storage bucket.

        Args:
            days (int): The number of days to look back for log analysis.

        Returns:
            List[Dict[str, Any]]: A list of log entries that match the specified time range.
        """
        found_events = []
        storage_client = storage.Client(project=self.project_id)

        now = datetime.now(timezone.utc)
        end_time = now.replace(minute=0, second=0, microsecond=0) - timedelta(minutes=30)
        start_time = end_time - timedelta(days=days)

        blobs = storage_client.list_blobs(self.bucket)
        for blob in blobs:
            if not (start_time <= blob.time_created < end_time):
                continue

            self.logger.debug(f"Processing blob: {blob.name}")
            content = blob.download_as_string().decode("utf-8")

            for num, line in enumerate(content.splitlines(), 1):
                try:
                    log_entry = json.loads(line)
                    payload = log_entry.get("protoPayload")
                    if not payload:
                        self.logger.warning(f"Skipping log in blob {blob.name}, line {num}: no protoPayload found.")
                        continue

                    event_details = {
                        "timestamp": log_entry.get("timestamp", "N/A"),
                        "principal": payload.get("authenticationInfo", {}).get("principalEmail", "N/A"),
                        "method": payload.get("methodName", "N/A"),
                        "resource": payload.get("resourceName", "N/A"),
                        "project_id": log_entry.get("resource", {}).get("labels", {}).get("project_id", "N/A"),
                        "file_name": blob.name
                    }
                    found_events.append(event_details)
                except json.JSONDecodeError:
                    self.logger.warning(f"Skipping invalid JSON log in blob {blob.name}, line {num}.")
                    continue

        storage_client.close()
        return found_events

    def create_weekly_email_report(self, dry_run: bool = False) -> None:
        """
        Creates an email report based on the events found this week.
        If `dry_run` is True, it will print the report to the console instead of sending it.
        """
        events = self.get_event_logs(days=7)
        if not events:
            self.logger.info("No events found for the weekly report.")
            return

        events.sort(key=lambda x: x['timestamp'], reverse=True)
        event_summary = "\n".join(
            f"Timestamp: {event['timestamp']}, Principal: {event['principal']}, Method: {event['method']}, Resource: {event['resource']}, Project ID: {event['project_id']}, File: {event['file_name']}"
            for event in events
        )

        report_subject = REPORT_SUBJECT
        report_body = REPORT_BODY_TEMPLATE.format(event_summary=event_summary)

        if dry_run:
            self.logger.info("Dry run: printing email report to console.")
            print(f"Subject: {report_subject}\n")
            print(f"Body:\n{report_body}")
            return

        self.send_email(report_subject, report_body)

    def send_email(self, subject: str, body: str) -> None:
        """
        Sends an email with the specified subject and body.
        If email configuration is not fully set, it prints the email instead.

        Args:
            subject (str): The subject of the email.
            body (str): The body of the email.
        """
        smtp_server = os.getenv("SMTP_SERVER")
        smtp_port_str = os.getenv("SMTP_PORT")
        recipient = os.getenv("EMAIL_RECIPIENT")
        email = os.getenv("EMAIL_ADDRESS")
        password = os.getenv("EMAIL_PASSWORD")

        if not all([smtp_server, smtp_port_str, recipient, email, password]):
            self.logger.warning("Email configuration is not fully set. Printing email instead.")
            print(f"Subject: {subject}\n")
            print(f"Body:\n{body}")
            return

        assert smtp_server is not None
        assert smtp_port_str is not None
        assert recipient is not None
        assert email is not None
        assert password is not None

        message = f"Subject: {subject}\n\n{body}"
        context = ssl.create_default_context()

        try:
            smtp_port = int(smtp_port_str)
            with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
                server.login(email, password)
                server.sendmail(email, recipient, message)
            self.logger.info(f"Successfully sent email report to {recipient}")
        except Exception as e:
            self.logger.error(f"Failed to send email report: {e}")

def load_config_from_yaml(config_path: str) -> Dict[str, Any]:
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    c = {
        "project_id": config.get("project_id"),
        "gcp_bucket": config.get("bucket_name"),
        "sinks": [],
        "logger": logging.getLogger(__name__)
    }

    for sink_config in config.get("sinks", []):
        sink = SinkCls(
            name=sink_config["name"],
            description=sink_config["description"],
            filter_methods=sink_config.get("filter_methods", []),
            excluded_principals=sink_config.get("excluded_principals", [])
        )
        c["sinks"].append(sink)

    logging_config = config.get("logging", {})
    log_level = logging_config.get("level", "INFO")
    log_format = logging_config.get("format", "[%(asctime)s] %(levelname)s: %(message)s")

    c["logger"].setLevel(log_level)
    logging.basicConfig(level=log_level, format=log_format)

    return c

def main():
    """
    Main entry point for the script.
    """
    parser = argparse.ArgumentParser(description="GCP IAM Log Analyzer")
    parser.add_argument("--config", required=True, help="Path to the configuration YAML file.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("initialize", help="Initialize/update log sinks in GCP.")
    report_parser = subparsers.add_parser("generate-report", help="Generate and send the weekly IAM security report.")
    report_parser.add_argument("--dry-run", action="store_true", help="Do not send email, print report to console.")

    args = parser.parse_args()

    config = load_config_from_yaml(args.config)
    log_analyzer = LogAnalyzer(
        project_id=config["project_id"],
        gcp_bucket=config["gcp_bucket"],
        logger=config["logger"],
        sinks=config["sinks"]
    )

    if args.command == "initialize":
        log_analyzer.initialize_sinks()
        log_analyzer.logger.info("Sinks initialized successfully.")
    elif args.command == "generate-report":
        log_analyzer.create_weekly_email_report(dry_run=args.dry_run)
        log_analyzer.logger.info("Weekly report generation process completed.")

if __name__ == "__main__":
    main()
