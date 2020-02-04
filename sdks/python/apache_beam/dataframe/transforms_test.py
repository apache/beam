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

import pandas as pd

import apache_beam as beam
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import transforms
from apache_beam.testing.util import assert_that


class TransformTest(unittest.TestCase):
  def run_test(self, input, func):
    expected = func(input)

    empty = input[0:0]
    input_placeholder = expressions.PlaceholderExpression(empty)
    input_deferred = frame_base.DeferredFrame.wrap(input_placeholder)
    actual_deferred = func(input_deferred)._expr.evaluate_at(
        expressions.Session({input_placeholder: input}))

    def check_correct(actual):
      if actual is None:
        raise AssertionError('Empty frame but expected: \n\n%s' % (expected))
      sorted_actual = actual.sort_index()
      sorted_expected = expected.sort_index()
      if not sorted_actual.equals(sorted_expected):
        raise AssertionError(
            'Dataframes not equal: \n\n%s\n\n%s' %
            (sorted_actual, sorted_expected))

    check_correct(actual_deferred)

    with beam.Pipeline() as p:
      input_pcoll = p | beam.Create([input[::2], input[1::2]])
      output_pcoll = input_pcoll | transforms.DataframeTransform(
          func, proxy=empty)
      assert_that(
          output_pcoll,
          lambda actual: check_correct(pd.concat(actual) if actual else None))

  def test_identity(self):
    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self.run_test(df, lambda x: x)

  def test_sum_mean(self):
    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self.run_test(df, lambda df: df.groupby('Animal').sum())
    self.run_test(df, lambda df: df.groupby('Animal').mean())

  def test_filter(self):
    df = pd.DataFrame({
        'Animal': ['Aardvark', 'Ant', 'Elephant', 'Zebra'],
        'Speed': [5, 2, 35, 40]
    })
    self.run_test(df, lambda df: df.filter(items=['Animal']))
    self.run_test(df, lambda df: df.filter(regex='A.*'))
    self.run_test(
        df, lambda df: df.set_index('Animal').filter(regex='F.*', axis='index'))


if __name__ == '__main__':
  unittest.main()
