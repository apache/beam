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

from __future__ import absolute_import
from __future__ import print_function

import unittest

import mock

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline

from . import min as beam_min


def check_min_element(actual):
  expected = '''[START min_element]
1
[END min_element]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_elements_with_min_value_per_key(actual):
  expected = '''[START elements_with_min_value_per_key]
('🥕', 2)
('🍆', 1)
('🍅', 3)
[END elements_with_min_value_per_key]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.min.print', str)
class MinTest(unittest.TestCase):
  def test_min_globally(self):
    beam_min.min_globally(check_min_element)

  def test_min_per_key(self):
    beam_min.min_per_key(check_elements_with_min_value_per_key)


if __name__ == '__main__':
  unittest.main()
