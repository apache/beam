#
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
#
# pytype: skip-file

import unittest

try:
  from google.api_core.exceptions import TooManyRequests

  from apache_beam.ml.inference.vertex_ai_inference import VertexAIModelHandlerJSON
  from apache_beam.ml.inference.vertex_ai_inference import _retry_on_appropriate_gcp_error
except ImportError:
  raise unittest.SkipTest('VertexAI dependencies are not installed')


class RetryOnClientErrorTest(unittest.TestCase):
  def test_retry_on_client_error_positive(self):
    e = TooManyRequests(message="fake service rate limiting")
    self.assertTrue(_retry_on_appropriate_gcp_error(e))

  def test_retry_on_client_error_negative(self):
    e = ValueError()
    self.assertFalse(_retry_on_appropriate_gcp_error(e))


class ModelHandlerArgConditions(unittest.TestCase):
  def test_exception_on_private_without_network(self):
    self.assertRaises(
        ValueError,
        VertexAIModelHandlerJSON,
        endpoint_id="1",
        project="testproject",
        location="us-central1",
        private=True)


class ParseInvokeResponseTest(unittest.TestCase):
  """Tests for _parse_invoke_response method."""
  def _create_handler_with_invoke_route(self, invoke_route="/test"):
    """Creates a mock handler with invoke_route for testing."""
    import unittest.mock as mock
    with mock.patch.object(VertexAIModelHandlerJSON,
                           '_retrieve_endpoint',
                           return_value=None):
      handler = VertexAIModelHandlerJSON(
          endpoint_id="1",
          project="testproject",
          location="us-central1",
          invoke_route=invoke_route)
    return handler

  def test_parse_invoke_response_with_predictions_key(self):
    """Test parsing response with standard 'predictions' key."""
    handler = self._create_handler_with_invoke_route()
    batch = [{"input": "test1"}, {"input": "test2"}]
    response = (
        b'{"predictions": ["result1", "result2"], '
        b'"deployedModelId": "model123"}')

    results = list(handler._parse_invoke_response(batch, response))

    self.assertEqual(len(results), 2)
    self.assertEqual(results[0].example, {"input": "test1"})
    self.assertEqual(results[0].inference, "result1")
    self.assertEqual(results[1].example, {"input": "test2"})
    self.assertEqual(results[1].inference, "result2")

  def test_parse_invoke_response_list_format(self):
    """Test parsing response as a list of predictions."""
    handler = self._create_handler_with_invoke_route()
    batch = [{"input": "test1"}, {"input": "test2"}]
    response = b'["result1", "result2"]'

    results = list(handler._parse_invoke_response(batch, response))

    self.assertEqual(len(results), 2)
    self.assertEqual(results[0].inference, "result1")
    self.assertEqual(results[1].inference, "result2")

  def test_parse_invoke_response_single_prediction(self):
    """Test parsing response with a single prediction."""
    handler = self._create_handler_with_invoke_route()
    batch = [{"input": "test1"}]
    response = b'{"output": "single result"}'

    results = list(handler._parse_invoke_response(batch, response))

    self.assertEqual(len(results), 1)
    self.assertEqual(results[0].inference, {"output": "single result"})

  def test_parse_invoke_response_non_json(self):
    """Test handling non-JSON response."""
    handler = self._create_handler_with_invoke_route()
    batch = [{"input": "test1"}]
    response = b'not valid json'

    results = list(handler._parse_invoke_response(batch, response))

    self.assertEqual(len(results), 1)
    self.assertEqual(results[0].inference, response)


if __name__ == '__main__':
  unittest.main()
