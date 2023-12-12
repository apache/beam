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
import time
import unittest

import apache_beam as beam
from apache_beam.io.requestresponseio import Caller
from apache_beam.io.requestresponseio import RequestResponseIO
from apache_beam.io.requestresponseio import UserCodeExecutionException
from apache_beam.io.requestresponseio import UserCodeTimeoutException
from apache_beam.testing.test_pipeline import TestPipeline


class AckCaller(Caller):
  """AckCaller acknowledges the incoming request by returning a
  request with ACK."""
  def __enter__(self):
    pass

  def __call__(self, request: str):
    return f"ACK: {request}"

  def __exit__(self, exc_type, exc_val, exc_tb):
    return None


class CallerWithTimeout(AckCaller):
  """CallerWithTimeout sleeps for 2 seconds before responding.
  Used to test timeout in RequestResponseIO."""
  def __call__(self, request: str, *args, **kwargs):
    time.sleep(2)
    return f"ACK: {request}"


class CallerWithRuntimeError(AckCaller):
  """CallerWithRuntimeError raises a `RuntimeError` for RequestResponseIO
  to raise a UserCodeExecutionException."""
  def __call__(self, request: str, *args, **kwargs):
    if not request:
      raise RuntimeError("Exception expected, not an error.")


class TestCaller(unittest.TestCase):
  def test_valid_call(self):
    caller = AckCaller()
    with TestPipeline() as test_pipeline:
      output = (
          test_pipeline
          | beam.Create(["sample_request"])
          | RequestResponseIO(caller=caller))

    self.assertIsNotNone(output)

  def test_call_timeout(self):
    caller = CallerWithTimeout()
    with self.assertRaises(UserCodeTimeoutException):
      with TestPipeline() as test_pipeline:
        _ = (
            test_pipeline
            | beam.Create(["timeout_request"])
            | RequestResponseIO(caller=caller, timeout=1))

  def test_call_runtime_error(self):
    caller = CallerWithRuntimeError()
    with self.assertRaises(UserCodeExecutionException):
      with TestPipeline() as test_pipeline:
        _ = (
            test_pipeline
            | beam.Create([""])
            | RequestResponseIO(caller=caller))


if __name__ == '__main__':
  unittest.main()
