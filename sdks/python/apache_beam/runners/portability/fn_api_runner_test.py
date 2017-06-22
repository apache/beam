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
from apache_beam.runners.portability import fn_api_runner
from apache_beam.runners.portability import maptask_executor_runner_test


class FnApiRunnerTest(
    maptask_executor_runner_test.MapTaskExecutorRunnerTest):

  def create_pipeline(self):
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner())

  def test_combine_per_key(self):
    # TODO(robertwb): Implement PGBKCV operation.
    pass

  # Inherits all tests from maptask_executor_runner.MapTaskExecutorRunner


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
