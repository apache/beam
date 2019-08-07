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

from apache_beam.examples.snippets.transforms.element_wise.regex import *
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


@mock.patch('apache_beam.Pipeline', TestPipeline)
# pylint: disable=line-too-long
@mock.patch('apache_beam.examples.snippets.transforms.element_wise.regex.print', lambda elem: elem)
# pylint: enable=line-too-long
class RegexTest(unittest.TestCase):
  def __init__(self, methodName):
    super(RegexTest, self).__init__(methodName)
    # pylint: disable=line-too-long
    # [START plant_matches]
    plant_matches = [
        {'match': '🍓   -   Strawberry   -   perennial', 'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'},
        {'match': '🥕 - Carrot - biennial', 'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'},
        {'match': '🍆\t-\tEggplant\t-\tperennial', 'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'},
        {'match': '🍅 - Tomato - annual', 'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'},
        {'match': '🥔-Potato-perennial', 'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'},
    ]
    # [END plant_matches]
    # pylint: enable=line-too-long
    self.plant_matches_test = lambda actual: \
        assert_that(actual, equal_to(plant_matches))

    # [START words]
    words = [
        '🍓',
        'Strawberry',
        'perennial',
        '🥕',
        'Carrot',
        'biennial',
        '🍆',
        'Eggplant',
        'perennial',
        '🍅',
        'Tomato',
        'annual',
        '🥔',
        'Potato',
        'perennial',
    ]
    # [END words]
    self.words_test = lambda actual: assert_that(actual, equal_to(words))

    # [START plants_csv]
    plants_csv = [
        '🍓,Strawberry,perennial',
        '🥕,Carrot,biennial',
        '🍆,Eggplant,perennial',
        '🍅,Tomato,annual',
        '🥔,Potato,perennial',
    ]
    # [END plants_csv]
    self.plants_csv_test = lambda actual: \
        assert_that(actual, equal_to(plants_csv))

    # [START plants_columns]
    plants_columns = [
        ['🍓', 'Strawberry', 'perennial'],
        ['🥕', 'Carrot', 'biennial'],
        ['🍆', 'Eggplant', 'perennial'],
        ['🍅', 'Tomato', 'annual'],
        ['🥔', 'Potato', 'perennial'],
    ]
    # [END plants_columns]
    self.plants_columns_test = lambda actual: \
        assert_that(actual, equal_to(plants_columns))

  def test_regex_match(self):
    regex_match(self.plant_matches_test)

  def test_regex_search(self):
    regex_search(self.plant_matches_test)

  def test_regex_find_all(self):
    regex_find_all(self.words_test)

  def test_regex_replace(self):
    regex_replace(self.plants_csv_test)

  def test_regex_split(self):
    regex_split(self.plants_columns_test)


if __name__ == '__main__':
  unittest.main()
