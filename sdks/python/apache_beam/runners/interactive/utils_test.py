#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import

import unittest

import pandas as pd

from apache_beam.runners.interactive import utils
from apache_beam.typehints.typehints import Any
from apache_beam.typehints.typehints import Dict
from apache_beam.typehints.typehints import List
from apache_beam.typehints.typehints import Tuple
from apache_beam.utils.windowed_value import WindowedValue


class ParseToDataframeTest(unittest.TestCase):
  def test_parse_tuple(self):
    el = (1, 'a')
    element_type = Tuple[int, str]

    columns, elements = utils.parse_row(el, element_type)
    self.assertEqual(columns, ['el[0]', 'el[1]'])
    self.assertEqual(elements, [1, 'a'])

  def test_parse_nested_tuple(self):
    el = ((1, 2.0, 'a'), 'b')
    element_type = Tuple[Tuple[int, float, str], str]

    columns, elements = utils.parse_row(el, element_type)
    self.assertEqual(columns, ['el[0][0]', 'el[0][1]', 'el[0][2]', 'el[1]'])
    self.assertEqual(elements, [1, 2.0, 'a', 'b'])

  def test_parse_list(self):
    el = [1, 2, 3]
    element_type = List[int]

    columns, elements = utils.parse_row(el, element_type)
    self.assertEqual(columns, ['el'])
    self.assertEqual(elements, [[1, 2, 3]])

  def test_parse_nested_list(self):
    el = ('k', [1, 2, 3])
    element_type = Tuple[str, List[int]]

    columns, elements = utils.parse_row(el, element_type)
    self.assertEqual(columns, ['el[0]', 'el[1]'])
    self.assertEqual(elements, ['k', [1, 2, 3]])

  def test_parse_complex(self):
    el = (([1, 2, 3], {'b': 1, 'c': 2}), 'a')
    element_type = Tuple[Tuple[List[int], Dict[str, int]], str]

    columns, elements = utils.parse_row(el, element_type)
    self.assertEqual(columns, ['el[0][0]', 'el[0][1]', 'el[1]'])
    self.assertEqual(elements, [[1, 2, 3], {'b': 1, 'c': 2}, 'a'])

  def test_parse_singleton(self):
    el = 1
    element_type = int

    columns, elements = utils.parse_row(el, element_type)
    self.assertEqual(columns, ['el'])
    self.assertEqual(elements, [1])

  def test_parse_namedtuple(self):
    """Tests that namedtuple are supported and have their own columns.
    """
    from collections import namedtuple

    KVCount = namedtuple('KeyCount', ['word', 'count'])

    el = KVCount(word='a', count=2)
    element_type = Any

    columns, elements = utils.parse_row(el, element_type)
    self.assertEqual(columns, ['word', 'count'])
    self.assertEqual(elements, ['a', 2])

  def test_parse_nestednamedtuple(self):
    """Tests that nested namedtuples are indeed.
    """
    from collections import namedtuple

    KVCount = namedtuple('KeyCount', ['word', 'count'])

    el = ('k', KVCount(word='a', count=2))
    element_type = Tuple[str, Any]

    columns, elements = utils.parse_row(el, element_type)
    self.assertEqual(columns, ['el[0]', 'el[1][word]', 'el[1][count]'])
    self.assertEqual(elements, ['k', 'a', 2])

  def test_parse_windowedvalue(self):
    """Tests that WindowedValues are supported and have their own columns.
    """
    from apache_beam.transforms.window import GlobalWindow

    el = WindowedValue(('a', 2), 1, [GlobalWindow()])
    element_type = Tuple[str, int]

    columns, elements = utils.parse_row(el, element_type)
    self.assertEqual(
        columns, ['el[0]', 'el[1]', 'event_time', 'windows', 'pane_info'])
    self.assertEqual(elements, ['a', 2, 1e6, el.windows, el.pane_info])

  def test_parse_dataframe(self):
    from collections import namedtuple

    KVCount = namedtuple('KeyCount', ['word', 'count'])

    els = [('k', KVCount(word='a', count=1)), ('k', KVCount(word='b', count=2)),
           ('k', KVCount(word='c', count=3))]
    element_type = Tuple[str, Any]

    actual_df = utils.pcoll_to_df(els, element_type)
    expected_df = pd.DataFrame([['k', 'a', 1], ['k', 'b', 2], ['k', 'c', 3]],
                               columns=['el[0]', 'el[1][word]', 'el[1][count]'])
    pd.testing.assert_frame_equal(actual_df, expected_df)


if __name__ == '__main__':
  unittest.main()
