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

from __future__ import absolute_import

import unittest

import numpy as np
import pandas as pd

from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import frames  # pylint: disable=unused-import


class DeferredFrameTest(unittest.TestCase):
  def _run_test(self, func, *args):
    deferred_args = [
        frame_base.DeferredFrame.wrap(
            expressions.ConstantExpression(arg, arg[0:0])) for arg in args
    ]
    expected = func(*args)
    actual = expressions.Session({}).evaluate(func(*deferred_args)._expr)
    self.assertTrue(
        expected.equals(actual),
        'Expected:\n\n%r\n\nActual:\n\n%r' % (expected, actual))

  def test_series_arithmetic(self):
    a = pd.Series([1, 2, 3])
    b = pd.Series([100, 200, 300])
    self._run_test(lambda a, b: a - 2 * b, a, b)

  def test_get_column(self):
    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self._run_test(lambda df: df['Animal'], df)
    self._run_test(lambda df: df.Speed, df)

  def test_set_column(self):
    def new_column(df):
      df['NewCol'] = df['Speed']
      return df

    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self._run_test(new_column, df)

  def test_groupby(self):
    df = pd.DataFrame({'group': ['a', 'a', 'a', 'b'], 'value': [1, 2, 3, 5]})
    self._run_test(lambda df: df.groupby('group').agg(sum), df)
    self._run_test(lambda df: df.groupby('group').sum(), df)
    self._run_test(lambda df: df.groupby('group').median(), df)

  def test_loc(self):
    dates = pd.date_range('1/1/2000', periods=8)
    df = pd.DataFrame(
        np.arange(32).reshape((8, 4)),
        index=dates,
        columns=['A', 'B', 'C', 'D'])
    self._run_test(lambda df: df.loc[:], df)
    self._run_test(lambda df: df.loc[:, 'A'], df)
    self._run_test(lambda df: df.loc[:dates[3]], df)
    self._run_test(lambda df: df.loc[df.A > 10], df)
    self._run_test(lambda df: df.loc[lambda df: df.A > 10], df)


if __name__ == '__main__':
  unittest.main()
