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

from __future__ import absolute_import
from __future__ import print_function

import sys
import unittest

import mock

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline

from . import combineglobally


def check_common_items(actual):
  expected = '''[START common_items]
{'🍅', '🥕'}
[END common_items]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_common_items_with_exceptions(actual):
  expected = '''[START common_items_with_exceptions]
{'🍅'}
[END common_items_with_exceptions]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_custom_common_items(actual):
  expected = '''[START custom_common_items]
{'🍅', '🍇', '🌽'}
[END custom_common_items]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_percentages(actual):
  expected = '''[START percentages]
{'🥕': 0.3, '🍅': 0.6, '🍆': 0.1}
[END percentages]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
# pylint: disable=line-too-long
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.combineglobally.print',
    str)
# pylint: enable=line-too-long
class CombineGloballyTest(unittest.TestCase):
  # TODO: Remove this after Python 2 deprecation.
  # https://issues.apache.org/jira/browse/BEAM-8124
  @unittest.skipIf(
      sys.version_info[0] == 2, 'Python 2 renders sets in a non-compatible way')
  def test_combineglobally_function(self):
    combineglobally.combineglobally_function(check_common_items)

  # TODO: Remove this after Python 2 deprecation.
  # https://issues.apache.org/jira/browse/BEAM-8124
  @unittest.skipIf(
      sys.version_info[0] == 2, 'Python 2 renders sets in a non-compatible way')
  def test_combineglobally_lambda(self):
    combineglobally.combineglobally_lambda(check_common_items)

  # TODO: Remove this after Python 2 deprecation.
  # https://issues.apache.org/jira/browse/BEAM-8124
  @unittest.skipIf(
      sys.version_info[0] == 2, 'Python 2 renders sets in a non-compatible way')
  def test_combineglobally_multiple_arguments(self):
    combineglobally.combineglobally_multiple_arguments(
        check_common_items_with_exceptions)

  # TODO: Remove this after Python 2 deprecation.
  # https://issues.apache.org/jira/browse/BEAM-8124
  @unittest.skipIf(
      sys.version_info[0] == 2, 'Python 2 renders sets in a non-compatible way')
  def test_combineglobally_side_inputs_singleton(self):
    combineglobally.combineglobally_side_inputs_singleton(
        check_common_items_with_exceptions)

  # TODO: enable side inputs tests after [BEAM-8400] is fixed.
  # https://issues.apache.org/jira/browse/BEAM-8400
  # def test_combineglobally_side_inputs_iter(self):
  #   combineglobally.combineglobally_side_inputs_iter(
  #       check_common_items_with_exceptions)

  # def test_combineglobally_side_inputs_dict(self):
  #   combineglobally.combineglobally_side_inputs_dict(
  #       check_custom_common_items)

  # TODO: Remove this after Python 2 deprecation.
  # https://issues.apache.org/jira/browse/BEAM-8124
  @unittest.skipIf(
      sys.version_info[0] == 2, 'Python 2 renders sets in a non-compatible way')
  def test_combineglobally_combinefn(self):
    combineglobally.combineglobally_combinefn(check_percentages)


if __name__ == '__main__':
  unittest.main()
