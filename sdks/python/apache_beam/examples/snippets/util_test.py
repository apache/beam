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

from mock import patch

import apache_beam as beam
from apache_beam.examples.snippets import util
from apache_beam.testing.test_pipeline import TestPipeline


class UtilTest(unittest.TestCase):
  def test_assert_matches_stdout_object(self):
    expected = [
        "{'a': '🍓', 'b': True}",
        "{'a': '🥕', 'b': 42}",
        "{'a': '🍆', 'b': '\"hello\"'}",
        "{'a': '🍅', 'b': [1, 2, 3]}",
        "{'b': 'B', 'a': '🥔'}",
    ]
    with TestPipeline() as pipeline:
      actual = (
          pipeline
          | beam.Create([
              {
                  'a': '🍓', 'b': True
              },
              {
                  'a': '🥕', 'b': 42
              },
              {
                  'a': '🍆', 'b': '"hello"'
              },
              {
                  'a': '🍅', 'b': [1, 2, 3]
              },
              {
                  'a': '🥔', 'b': 'B'
              },
          ])
          | beam.Map(str))
      util.assert_matches_stdout(actual, expected)

  def test_assert_matches_stdout_string(self):
    expected = ['🍓', '🥕', '🍆', '🍅', '🥔']
    with TestPipeline() as pipeline:
      actual = (
          pipeline
          | beam.Create(['🍓', '🥕', '🍆', '🍅', '🥔'])
          | beam.Map(str))
      util.assert_matches_stdout(actual, expected)

  def test_assert_matches_stdout_sorted_keys(self):
    expected = [{'list': [1, 2]}, {'list': [3, 4]}]
    with TestPipeline() as pipeline:
      actual = (
          pipeline
          | beam.Create([{
              'list': [2, 1]
          }, {
              'list': [4, 3]
          }])
          | beam.Map(str))
      util.assert_matches_stdout(
          actual, expected, lambda elem: {'sorted': sorted(elem['list'])})

  @patch('subprocess.call', lambda cmd: None)
  def test_run_shell_commands(self):
    commands = [
        '  # this is a comment  ',
        '  !  echo   this   is   a   shell   command  ',
        '  !echo {variable}  ',
        '  echo "quoted arguments work"  # trailing comment  ',
    ]
    actual = list(util.run_shell_commands(commands, variable='hello world'))
    expected = [
        ['echo', 'this', 'is', 'a', 'shell', 'command'],
        ['echo', 'hello', 'world'],
        ['echo', 'quoted arguments work'],
    ]
    self.assertEqual(actual, expected)


if __name__ == '__main__':
  unittest.main()
