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
from unittest import mock

try:
  from google.genai import errors

  from apache_beam.ml.inference.gemini_inference import GeminiModelHandler
  from apache_beam.ml.inference.gemini_inference import _retry_on_appropriate_service_error
  from apache_beam.ml.inference.gemini_inference import generate_from_string
except ImportError:
  raise unittest.SkipTest('Gemini dependencies are not installed')


class RetryOnClientErrorTest(unittest.TestCase):
  def test_retry_on_client_error_positive(self):
    e = errors.APIError(code=429, response_json={})
    self.assertTrue(_retry_on_appropriate_service_error(e))

  def test_retry_on_client_error_negative(self):
    e = errors.APIError(code=404, response_json={})
    self.assertFalse(_retry_on_appropriate_service_error(e))

  def test_retry_on_server_error(self):
    e = errors.APIError(code=501, response_json={})
    self.assertTrue(_retry_on_appropriate_service_error(e))


class ModelHandlerArgConditions(unittest.TestCase):
  def test_all_params_set(self):
    self.assertRaises(
        ValueError,
        GeminiModelHandler,
        model_name="gemini-model-123",
        request_fn=generate_from_string,
        api_key="123456789",
        project="testproject",
        location="us-central1",
    )

  def test_missing_vertex_location_param(self):
    self.assertRaises(
        ValueError,
        GeminiModelHandler,
        model_name="gemini-model-123",
        request_fn=generate_from_string,
        project="testproject",
    )

  def test_missing_vertex_project_param(self):
    self.assertRaises(
        ValueError,
        GeminiModelHandler,
        model_name="gemini-model-123",
        request_fn=generate_from_string,
        location="us-central1",
    )

  def test_missing_all_params(self):
    self.assertRaises(
        ValueError,
        GeminiModelHandler,
        model_name="gemini-model-123",
        request_fn=generate_from_string,
    )


@unittest.mock.patch('apache_beam.ml.inference.gemini_inference.genai.Client')
@unittest.mock.patch('apache_beam.ml.inference.gemini_inference.HttpOptions')
class TestGeminiModelHandler(unittest.TestCase):
  def test_create_client_with_flex_api(
      self, mock_http_options, mock_genai_client):
    handler = GeminiModelHandler(
        model_name="gemini-pro",
        request_fn=generate_from_string,
        project="test-project",
        location="us-central1",
        use_vertex_flex_api=True)
    handler.create_client()
    mock_http_options.assert_called_with(
        api_version="v1",
        headers={"X-Vertex-AI-LLM-Request-Type": "flex"},
        timeout=600000,
    )
    mock_genai_client.assert_called_with(
        vertexai=True,
        project="test-project",
        location="us-central1",
        http_options=mock_http_options.return_value)


if __name__ == '__main__':
  unittest.main()
