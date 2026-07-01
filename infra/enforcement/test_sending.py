import unittest
from unittest.mock import patch, MagicMock
import logging
import requests
from sending import SendingClient

class TestSendingClient(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("TestLogger")
        self.logger.setLevel(logging.DEBUG)
        # Create a SendingClient with dummy values
        self.client = SendingClient(
            logger=self.logger,
            github_token="dummy-token",
            github_repo="apache/beam",
            smtp_server="smtp.example.com",
            smtp_port=587,
            email="sender@example.com",
            password="password"
        )

    @patch("requests.request")
    def test_get_open_issues_flaky_retry(self, mock_request):
        # We simulate a 403 secondary rate limit error on the first request,
        # and a successful 200 response on the second request.
        mock_response_fail = MagicMock()
        mock_response_fail.status_code = 403
        mock_response_fail.ok = False
        mock_response_fail.headers = {"Retry-After": "1"}
        mock_response_fail.raise_for_status.side_effect = requests.exceptions.HTTPError("403 Client Error")
        
        mock_response_success = MagicMock()
        mock_response_success.status_code = 200
        mock_response_success.ok = True
        mock_response_success.json.return_value = {
            "items": [
                {
                    "number": 1234,
                    "title": "[SECURITY] Action Required: Unmanaged Service Account Keys Detected",
                    "body": "Test body",
                    "state": "open",
                    "html_url": "https://github.com/apache/beam/issues/1234",
                    "created_at": "2026-07-02T00:00:00Z",
                    "updated_at": "2026-07-02T00:00:00Z"
                }
            ]
        }
        
        mock_request.side_effect = [mock_response_fail, mock_response_success]

        # Call get_open_issues
        issues = self.client._get_open_issues("[SECURITY] Action Required: Unmanaged Service Account Keys Detected")
        
        # Verify that two requests were made (one retry)
        self.assertEqual(mock_request.call_count, 2)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].number, 1234)

    @patch("requests.request")
    def test_get_open_issues_query_format(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = {"items": []}
        mock_request.return_value = mock_response

        title = "[SECURITY] Action Required: Unmanaged Service Account Keys Detected"
        self.client._get_open_issues(title)

        # Verify that the query parameter was passed correctly to requests
        mock_request.assert_called_once()
        args, kwargs = mock_request.call_args
        
        # Check that we passed params dict containing 'q'
        self.assertIn("params", kwargs)
        self.assertIn("q", kwargs["params"])
        
        q = kwargs["params"]["q"]
        # The query should specify the repo, title (quoted), state and type
        self.assertIn('repo:apache/beam', q)
        self.assertIn('is:issue', q)
        self.assertIn('is:open', q)
        self.assertIn('in:title', q)
        self.assertIn(f'"{title}"', q)


if __name__ == "__main__":
    unittest.main()
