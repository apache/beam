# coding=utf-8
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

import mock

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline

from . import mean_globally
from . import mean_per_key


def check_mean_element(actual):
  expected = '''[START mean_element]
2.5
[END mean_element]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_elements_with_mean_value_per_key(actual):
  expected = '''[START elements_with_mean_value_per_key]
('🥕', 2.5)
('🍆', 1.0)
('🍅', 4.0)
[END elements_with_mean_value_per_key]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.mean_globally.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.mean_per_key.print',
    str)
class MeanTest(unittest.TestCase):
  def test_mean_globally(self):
    mean_globally.mean_globally(check_mean_element)

  def test_mean_per_key(self):
    mean_per_key.mean_per_key(check_elements_with_mean_value_per_key)


if __name__ == '__main__':
  unittest.main()
