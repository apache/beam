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
  from apache_beam.ml.inference.vertex_ai_inference import _retry_on_appropriate_gcp_error
  from apache_beam.ml.inference.vertex_ai_inference import VertexAIModelHandlerJSON
  from google.api_core.exceptions import TooManyRequests
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


if __name__ == '__main__':
  unittest.main()
