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
from io import StringIO

import mock

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from . import pardo


def check_plants(actual):
  expected = '''[START plants]
🍓Strawberry
🥕Carrot
🍆Eggplant
🍅Tomato
🥔Potato
[END plants]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_dofn_params(actual):
  # pylint: disable=line-too-long
  expected = '\n'.join(
      '''[START dofn_params]
# timestamp
type(timestamp) -> <class 'apache_beam.utils.timestamp.Timestamp'>
timestamp.micros -> 1584675660000000
timestamp.to_rfc3339() -> '2020-03-20T03:41:00Z'
timestamp.to_utc_datetime() -> datetime.datetime(2020, 3, 20, 3, 41)

# window
type(window) -> <class 'apache_beam.transforms.window.IntervalWindow'>
window.start -> Timestamp(1584675660) (2020-03-20 03:41:00)
window.end -> Timestamp(1584675690) (2020-03-20 03:41:30)
window.max_timestamp() -> Timestamp(1584675689.999999) (2020-03-20 03:41:29.999999)
[END dofn_params]'''.splitlines()[1:-1])
  # pylint: enable=line-too-long
  assert_that(actual, equal_to([expected]))


def check_dofn_methods(actual):
  # Return the expected stdout to check the ordering of the called methods.
  return '''[START results]
__init__
setup
start_bundle
* process: 🍓
* process: 🥕
* process: 🍆
* process: 🍅
* process: 🥔
* finish_bundle: 🌱🌳🌍
teardown
[END results]'''.splitlines()[1:-1]


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.pardo.print', str)
class ParDoTest(unittest.TestCase):
  def test_pardo_dofn(self):
    pardo.pardo_dofn(check_plants)

  def test_pardo_dofn_params(self):
    pardo.pardo_dofn_params(check_dofn_params)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch('sys.stdout', new_callable=StringIO)
class ParDoStdoutTest(unittest.TestCase):
  def test_pardo_dofn_methods(self, mock_stdout):
    expected = pardo.pardo_dofn_methods(check_dofn_methods)
    actual = mock_stdout.getvalue().splitlines()

    # For the stdout, check the ordering of the methods, not of the elements.
    actual_stdout = [line.split(':')[0] for line in actual]
    expected_stdout = [line.split(':')[0] for line in expected]
    self.assertEqual(actual_stdout, expected_stdout)

    # For the elements, ignore the stdout and just make sure all elements match.
    actual_elements = {line for line in actual if line.startswith('*')}
    expected_elements = {line for line in expected if line.startswith('*')}
    self.assertEqual(actual_elements, expected_elements)


if __name__ == '__main__':
  unittest.main()
