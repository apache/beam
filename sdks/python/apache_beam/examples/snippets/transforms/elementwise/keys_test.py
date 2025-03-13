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

from . import keys


def check_icons(actual):
  expected = '''[START icons]
🍓
🥕
🍆
🍅
🥔
[END icons]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.keys.print', str)
class KeysTest(unittest.TestCase):
  def test_keys(self):
    keys.keys(check_icons)


if __name__ == '__main__':
  unittest.main()
