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

import ast
import unittest

import mock

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from . import cogroupbykey


def check_plants(actual):
  expected = '''[START plants]
('Apple', {'icons': ['🍎', '🍏'], 'durations': ['perennial']})
('Carrot', {'icons': [], 'durations': ['biennial']})
('Tomato', {'icons': ['🍅'], 'durations': ['perennial', 'annual']})
('Eggplant', {'icons': ['🍆'], 'durations': []})
[END plants]'''.splitlines()[1:-1]

  # Make it deterministic by sorting all sublists in each element.
  def normalize_element(plant_str):
    name, details = ast.literal_eval(plant_str)
    details['icons'] = sorted(details['icons'])
    details['durations'] = sorted(details['durations'])
    return name, details
  actual = actual | beam.Map(normalize_element)
  expected = [normalize_element(elem) for elem in expected]
  assert_that(actual, equal_to(expected))


@mock.patch('apache_beam.Pipeline', TestPipeline)
# pylint: disable=line-too-long
@mock.patch('apache_beam.examples.snippets.transforms.aggregation.cogroupbykey.print', str)
# pylint: enable=line-too-long
class CoGroupByKeyTest(unittest.TestCase):
  def test_cogroupbykey(self):
    cogroupbykey.cogroupbykey(check_plants)


if __name__ == '__main__':
  unittest.main()
