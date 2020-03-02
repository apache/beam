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

import numpy as np
import pandas as pd

from apache_beam.runners.interactive import utils
from apache_beam.utils.windowed_value import WindowedValue


class ParseToDataframeTest(unittest.TestCase):
  def test_parse_windowedvalue(self):
    """Tests that WindowedValues are supported but not present.
    """
    from apache_beam.transforms.window import GlobalWindow

    els = [
        WindowedValue(('a', 2), 1, [GlobalWindow()]),
        WindowedValue(('b', 3), 1, [GlobalWindow()])
    ]

    actual_df = utils.elements_to_df(els, include_window_info=False)
    expected_df = pd.DataFrame([['a', 2], ['b', 3]], columns=[0, 1])
    pd.testing.assert_frame_equal(actual_df, expected_df)

  def test_parse_windowedvalue_with_window_info(self):
    """Tests that WindowedValues are supported and have their own columns.
    """
    from apache_beam.transforms.window import GlobalWindow

    els = [
        WindowedValue(('a', 2), 1, [GlobalWindow()]),
        WindowedValue(('b', 3), 1, [GlobalWindow()])
    ]

    actual_df = utils.elements_to_df(els, include_window_info=True)
    expected_df = pd.DataFrame(
        [['a', 2, int(1e6), els[0].windows, els[0].pane_info],
         ['b', 3, int(1e6), els[1].windows, els[1].pane_info]],
        columns=[0, 1, 'event_time', 'windows', 'pane_info'])
    pd.testing.assert_frame_equal(actual_df, expected_df)

  def test_parse_windowedvalue_with_dicts(self):
    """Tests that dicts play well with WindowedValues.
    """
    from apache_beam.transforms.window import GlobalWindow

    els = [
        WindowedValue({
            'b': 2, 'd': 4
        }, 1, [GlobalWindow()]),
        WindowedValue({
            'a': 1, 'b': 2, 'c': 3
        }, 1, [GlobalWindow()])
    ]

    actual_df = utils.elements_to_df(els, include_window_info=True)
    expected_df = pd.DataFrame(
        [[np.nan, 2, np.nan, 4, int(1e6), els[0].windows, els[0].pane_info],
         [1, 2, 3, np.nan, int(1e6), els[1].windows, els[1].pane_info]],
        columns=['a', 'b', 'c', 'd', 'event_time', 'windows', 'pane_info'])
    pd.testing.assert_frame_equal(actual_df, expected_df)


if __name__ == '__main__':
  unittest.main()
