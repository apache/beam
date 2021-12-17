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

"""Tests for the various custom Count implementation examples."""

# pytype: skip-file

import logging
import unittest

import pytest

import apache_beam as beam
from apache_beam.examples.cookbook import custom_ptransform
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


@pytest.mark.examples_postcommit
class CustomCountTest(unittest.TestCase):
  def test_count1(self):
    self.run_pipeline(custom_ptransform.Count1())

  def test_count2(self):
    self.run_pipeline(custom_ptransform.Count2())

  def test_count3(self):
    factor = 2
    self.run_pipeline(custom_ptransform.Count3(factor), factor=factor)

  def run_pipeline(self, count_implementation, factor=1):
    with TestPipeline() as p:
      words = p | beam.Create(['CAT', 'DOG', 'CAT', 'CAT', 'DOG'])
      result = words | count_implementation
      assert_that(
          result, equal_to([('CAT', (3 * factor)), ('DOG', (2 * factor))]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
