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

from . import cogroupbykey


def check_plants(actual):
  expected = '''[START plants]
('Apple', {'icons': ['🍎', '🍏'], 'durations': ['perennial']})
('Carrot', {'icons': [], 'durations': ['biennial']})
('Tomato', {'icons': ['🍅'], 'durations': ['perennial', 'annual']})
('Eggplant', {'icons': ['🍆'], 'durations': []})
[END plants]'''.splitlines()[1:-1]

  # Make it deterministic by sorting all sublists in each element.
  def normalize_element(elem):
    name, details = elem
    details['icons'] = sorted(details['icons'])
    details['durations'] = sorted(details['durations'])
    return name, details

  assert_matches_stdout(actual, expected, normalize_element)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.cogroupbykey.print',
    str)
class CoGroupByKeyTest(unittest.TestCase):
  def test_cogroupbykey(self):
    cogroupbykey.cogroupbykey(check_plants)


if __name__ == '__main__':
  unittest.main()
