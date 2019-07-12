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

import unittest

from mock import patch

from apache_beam.examples.snippets.util import *


class UtilTest(unittest.TestCase):
  def test_parse_example_empty(self):
    # python path/to/snippets.py
    argv = []
    with self.assertRaises(SystemExit):
      self.assertEqual(parse_example(argv), 'example()')

  def test_parse_example_no_arguments(self):
    # python path/to/snippets.py example
    argv = ['example']
    self.assertEqual(parse_example(argv), 'example()')

  def test_parse_example_one_argument(self):
    # python path/to/snippets.py example A
    argv = ['example', 'A']
    self.assertEqual(parse_example(argv), "example('A')")

  def test_parse_example_multiple_arguments(self):
    # python path/to/snippets.py example A B "C's"
    argv = ['example', 'A', 'B', "C's"]
    self.assertEqual(parse_example(argv), "example('A', 'B', \"C's\")")

  @patch('subprocess.call', lambda cmd: None)
  def test_run_shell_commands(self):
    commands = [
        '  # this is a comment  ',
        '  !  echo   this   is   a   shell   command  ',
        '  !echo {variable}  ',
        '  echo "quoted arguments work"  # trailing comment  ',
    ]
    actual = list(run_shell_commands(commands, variable='hello world'))
    expected = [
        ['echo', 'this', 'is', 'a', 'shell', 'command'],
        ['echo', 'hello', 'world'],
        ['echo', 'quoted arguments work'],
    ]
    self.assertEqual(actual, expected)


if __name__ == '__main__':
  unittest.main()
