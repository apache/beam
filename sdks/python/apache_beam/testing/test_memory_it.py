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

"""Integration tests for memory consumption in the Python SDK."""

# pytype: skip-file
import argparse
import unittest
import psutil

import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline


def GetRssMb():
  rss = psutil.Process().memory_info().rss
  return rss / 1024 / 1024


class TestMemoryIntegrationTests(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.test_pipeline = TestPipeline(is_integration_test=True)
    cls.args = cls.test_pipeline.get_full_options_as_args()
    cls.runner_name = type(cls.test_pipeline.runner).__name__
    cls.project = cls.test_pipeline.get_option('project')

  @pytest.mark.it_postcommit
  def test_memory_execution(self):
    parser = argparse.ArgumentParser()
    pipeline_args = parser.parse_known_args()[1]
    before = GetRssMb()

    with beam.Pipeline(argv=pipeline_args) as p:
      _ = (p | beam.Create([1]) | beam.Map(lambda x: x + 1))
    after = GetRssMb()
    self.assertLess(after - before, 500)


if __name__ == '__main__':
  unittest.main()
