#!/usr/bin/env python
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

import json
import threading
import unittest
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock

import flaky_test_detection
from flaky_test_detection import get_grafana_alerts, get_workflow_issues


ALERT_JSON = [
    {
        "labels": {
            "workflow_id": "123",
            "workflow_url": "https://github.com/apache/beam/actions/workflows/foo.yml",
            "workflow_name": "Foo",
            "workflow_filename": "foo.yml",
            "workflow_threshold": "0.5",
            "dashboard_category": "misc",
            "workflow_retrieved_at": "2026-01-01T00:00:00+00:00",
        }
    }
]


class FakeGrafanaServer:
    """Local HTTP server that replies with the given (status, body) pairs in order."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.request_count = 0
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                server.request_count += 1
                status, body = server.responses.pop(0)
                payload = body.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_port}"

    def __enter__(self):
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.httpd.shutdown()
        self.httpd.server_close()


class GetGrafanaAlertsTest(unittest.TestCase):
    """Tests for get_grafana_alerts."""

    def fetch_alerts(self, responses):
        with FakeGrafanaServer(responses) as server:
            with mock.patch.object(flaky_test_detection, "GRAFANA_URL", server.url):
                try:
                    return get_grafana_alerts()
                finally:
                    self.request_count = server.request_count

    def test_parses_alerts(self):
        """Test that alerts are built from the Grafana response."""
        alerts = self.fetch_alerts([(200, json.dumps(ALERT_JSON))])

        self.assertEqual(self.request_count, 1)
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert.workflow_id, "123")
        self.assertEqual(alert.workflow_name, "Foo")
        self.assertEqual(alert.workflow_filename, "foo.yml")
        self.assertEqual(alert.workflow_threshold, 0.5)
        self.assertEqual(alert.workflow_dashboard_category, "misc")
        self.assertEqual(
            alert.workflow_retrieved_at,
            datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
        )

    def test_empty_response(self):
        """Test that an empty response yields no alerts."""
        alerts = self.fetch_alerts([(200, "")])

        self.assertEqual(alerts, [])

    def test_retries_on_server_error(self):
        """Test that transient server errors are retried."""
        alerts = self.fetch_alerts(
            [(503, "unavailable"), (200, json.dumps(ALERT_JSON))]
        )

        self.assertEqual(self.request_count, 2)
        self.assertEqual(len(alerts), 1)

    def test_raises_on_client_error(self):
        """Test that a non-retryable error fails without retrying."""
        with self.assertRaises(RuntimeError):
            self.fetch_alerts([(404, "not found")])

        self.assertEqual(self.request_count, 1)

    def test_uses_timeout(self):
        """Test that the request is made with a timeout."""
        response = mock.Mock(status_code=200, text="")
        with mock.patch("requests.Session.get", return_value=response) as get:
            get_grafana_alerts()

        self.assertEqual(
            get.call_args.kwargs["timeout"],
            flaky_test_detection.GRAFANA_REQUEST_TIMEOUT_SECONDS,
        )


class GetWorkflowIssuesTest(unittest.TestCase):
    """Tests for get_workflow_issues."""

    @staticmethod
    def make_issue(*label_names):
        issue = mock.Mock()
        issue.labels = []
        for name in label_names:
            label = mock.Mock()
            label.name = name
            issue.labels.append(label)
        return issue

    def test_maps_workflow_ids_to_issues(self):
        """Test that issues are keyed by the workflow id in their labels."""
        first = self.make_issue("flaky_test", "workflow_id: 111", "bug")
        second = self.make_issue("workflow_id:222", "P1")
        unrelated = self.make_issue("flaky_test")

        workflows = get_workflow_issues([first, second, unrelated])

        self.assertEqual(workflows, {"111": first, "222": second})

    def test_does_not_fetch_labels_per_issue(self):
        """Test that labels from the issue listing are used without extra requests."""
        issue = self.make_issue("workflow_id: 111")

        get_workflow_issues([issue])

        issue.get_labels.assert_not_called()


if __name__ == "__main__":
    unittest.main()
