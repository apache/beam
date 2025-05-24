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
  from apache_beam.ml.inference.gemini_inference import _retry_on_appropriate_service_error
  from apache_beam.ml.inference.gemini_inference import GeminiModelHandler
  from apache_beam.ml.inference.gemini_inference import generate_from_string
  from google.genai import errors
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


if __name__ == '__main__':
  unittest.main()
