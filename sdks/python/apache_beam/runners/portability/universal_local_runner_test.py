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

import logging
import unittest

import apache_beam as beam
from apache_beam.runners.portability import fn_api_runner_test
from apache_beam.runners.portability import universal_local_runner
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class UniversalLocalRunnerTest(fn_api_runner_test.FnApiRunnerTest):

  _use_grpc = False
  _use_subprocesses = False

  @classmethod
  def get_runner(cls):
    # Don't inherit.
    if '_runner' not in cls.__dict__:
      cls._runner = universal_local_runner.UniversalLocalRunner(
          use_grpc=cls._use_grpc,
          use_subprocesses=cls._use_subprocesses)
    return cls._runner

  @classmethod
  def tearDownClass(cls):
    cls._runner.cleanup()

  def create_pipeline(self):
    return beam.Pipeline(self.get_runner())

  def test_assert_that(self):
    # TODO: figure out a way for runner to parse and raise the
    # underlying exception.
    with self.assertRaises(Exception):
      with self.create_pipeline() as p:
        assert_that(p | beam.Create(['a', 'b']), equal_to(['a']))

  def test_errors(self):
    # TODO: figure out a way for runner to parse and raise the
    # underlying exception.
    with self.assertRaises(BaseException):
      with self.create_pipeline() as p:
        def raise_error(x):
          raise RuntimeError('x')
        # pylint: disable=expression-not-assigned
        (p
         | beam.Create(['a', 'b'])
         | 'StageA' >> beam.Map(lambda x: x)
         | 'StageB' >> beam.Map(lambda x: x)
         | 'StageC' >> beam.Map(raise_error)
         | 'StageD' >> beam.Map(lambda x: x))

  # Inherits all tests from fn_api_runner_test.FnApiRunnerTest


class UniversalLocalRunnerTestWithGrpc(UniversalLocalRunnerTest):
  _use_grpc = True


class UniversalLocalRunnerTestWithSubprocesses(UniversalLocalRunnerTest):
  _use_grpc = True
  _use_subprocesses = True


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
