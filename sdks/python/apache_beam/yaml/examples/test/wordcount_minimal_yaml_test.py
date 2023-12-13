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

from hamcrest.core import assert_that as hamcrest_assert
from hamcrest.library.collection import has_items

from apache_beam.testing.util import assert_that
from apache_beam.yaml.examples.test.test_yaml_example import test_yaml_example

YAML_FILE = '../wordcount_minimal.yaml'

EXPECTED = [
    "king: 311",
    "lear: 253",
    "dramatis: 1",
    "personae: 1",
    "of: 483",
    "britain: 2",
    "france: 32",
    "duke: 26",
    "burgundy: 20",
    "cornwall: 75"
]


def _contains_at_least(expected):
  def _matches(actual):
    hamcrest_assert(actual, has_items(*expected))

  return _matches


def _wordcount_matcher(actual, expected):
  return assert_that(actual, _contains_at_least(expected))


class WordCountMinimalYamlTest(unittest.TestCase):
  def test_wordcount_minimal_yaml(self):
    test_yaml_example(
        YAML_FILE, EXPECTED, element="output", matcher=_wordcount_matcher)


if __name__ == '__main__':
  unittest.main()
