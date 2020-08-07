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
from __future__ import division

import typing
import unittest

import pandas as pd
from past.builtins import unicode

import apache_beam as beam
from apache_beam import coders
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import transforms
from apache_beam.testing.util import assert_that


def sort_by_value_and_drop_index(df):
  if isinstance(df, pd.DataFrame):
    sorted_df = df.sort_values(by=list(df.columns))
  else:
    sorted_df = df.sort_values()
  return sorted_df.reset_index(drop=True)


def check_correct(expected, actual, check_index=False):
  if actual is None:
    raise AssertionError('Empty frame but expected: \n\n%s' % (expected))
  if isinstance(expected, pd.core.generic.NDFrame):
    sorted_actual = sort_by_value_and_drop_index(actual)
    sorted_expected = sort_by_value_and_drop_index(expected)
    if not sorted_actual.equals(sorted_expected):
      raise AssertionError(
          'Dataframes not equal: \n\n%s\n\n%s' %
          (sorted_actual, sorted_expected))
  else:
    if actual != expected:
      raise AssertionError('Scalars not equal: %s != %s' % (actual, expected))


def concat(parts):
  if len(parts) > 1:
    return pd.concat(parts)
  elif len(parts) == 1:
    return parts[0]
  else:
    return None


def df_equal_to(expected):
  return lambda actual: check_correct(expected, concat(actual))


AnimalSpeed = typing.NamedTuple(
    'AnimalSpeed', [('Animal', unicode), ('Speed', int)])
coders.registry.register_coder(AnimalSpeed, coders.RowCoder)


class TransformTest(unittest.TestCase):
  def run_scenario(self, input, func):
    expected = func(input)

    empty = input[0:0]
    input_placeholder = expressions.PlaceholderExpression(empty)
    input_deferred = frame_base.DeferredFrame.wrap(input_placeholder)
    actual_deferred = func(input_deferred)._expr.evaluate_at(
        expressions.Session({input_placeholder: input}))

    check_correct(expected, actual_deferred)

    with beam.Pipeline() as p:
      input_pcoll = p | beam.Create([input[::2], input[1::2]])
      output_pcoll = input_pcoll | transforms.DataframeTransform(
          func, proxy=empty)
      assert_that(
          output_pcoll, lambda actual: check_correct(expected, concat(actual)))

  def test_identity(self):
    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self.run_scenario(df, lambda x: x)

  def test_sum_mean(self):
    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    with expressions.allow_non_parallel_operations():
      self.run_scenario(df, lambda df: df.groupby('Animal').sum())
      self.run_scenario(df, lambda df: df.groupby('Animal').mean())

  def test_filter(self):
    df = pd.DataFrame({
        'Animal': ['Aardvark', 'Ant', 'Elephant', 'Zebra'],
        'Speed': [5, 2, 35, 40]
    })
    self.run_scenario(df, lambda df: df.filter(items=['Animal']))
    self.run_scenario(df, lambda df: df.filter(regex='A.*'))
    self.run_scenario(
        df, lambda df: df.set_index('Animal').filter(regex='F.*', axis='index'))

    with expressions.allow_non_parallel_operations():
      a = pd.DataFrame({'col': [1, 2, 3]})
      self.run_scenario(a, lambda a: a.agg(sum))
      self.run_scenario(a, lambda a: a.agg(['mean', 'min', 'max']))

  def test_scalar(self):
    with expressions.allow_non_parallel_operations():
      a = pd.Series([1, 2, 6])
      self.run_scenario(a, lambda a: a.agg(sum))
      self.run_scenario(a, lambda a: a / a.agg(sum))

      # Tests scalar being used as an input to a downstream stage.
      df = pd.DataFrame({'key': ['a', 'a', 'b'], 'val': [1, 2, 6]})
      self.run_scenario(
          df, lambda df: df.groupby('key').sum().val / df.val.agg(sum))

  def test_batching_named_tuple_input(self):
    with beam.Pipeline() as p:
      result = (
          p | beam.Create([
              AnimalSpeed('Aardvark', 5),
              AnimalSpeed('Ant', 2),
              AnimalSpeed('Elephant', 35),
              AnimalSpeed('Zebra', 40)
          ]).with_output_types(AnimalSpeed)
          | transforms.DataframeTransform(lambda df: df.filter(regex='A.*')))

      assert_that(
          result,
          df_equal_to(
              pd.DataFrame({'Animal': ['Aardvark', 'Ant', 'Elephant',
                                       'Zebra']})))

  def test_batching_beam_row_input(self):
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create([(u'Falcon', 380.), (u'Falcon', 370.), (u'Parrot', 24.),
                         (u'Parrot', 26.)])
          | beam.Map(lambda tpl: beam.Row(Animal=tpl[0], Speed=tpl[1]))
          |
          transforms.DataframeTransform(lambda df: df.groupby('Animal').mean()))

      assert_that(
          result,
          df_equal_to(
              pd.DataFrame({
                  'Animal': ['Falcon', 'Parrot'], 'Speed': [375., 25.]
              }).set_index('Animal')))

  def test_batching_unsupported_nested_schema_raises(self):
    Nested = typing.NamedTuple(
        'Nested', [('id', int), ('animal_speed', AnimalSpeed)])
    coders.registry.register_coder(Nested, coders.RowCoder)

    with beam.Pipeline() as p:
      nested_schema_pc = (
          p | beam.Create([Nested(1, AnimalSpeed('Aardvark', 5))
                           ]).with_output_types(Nested))
      with self.assertRaisesRegex(TypeError, 'animal_speed'):
        nested_schema_pc | transforms.DataframeTransform(  # pylint: disable=expression-not-assigned
            lambda df: df.filter(items=['id']))

  def test_batching_unsupported_array_schema_raises(self):
    Array = typing.NamedTuple(
        'Array', [('id', int), ('business_numbers', typing.Sequence[int])])
    coders.registry.register_coder(Array, coders.RowCoder)

    with beam.Pipeline() as p:
      array_schema_pc = (p | beam.Create([Array(1, [7, 8, 9])]))
      with self.assertRaisesRegex(TypeError, 'business_numbers'):
        array_schema_pc | transforms.DataframeTransform(  # pylint: disable=expression-not-assigned
            lambda df: df.filter(items=['id']))

  def test_input_output_polymorphism(self):
    one_series = pd.Series([1])
    two_series = pd.Series([2])
    three_series = pd.Series([3])
    proxy = one_series[:0]

    def equal_to_series(expected):
      def check(actual):
        actual = pd.concat(actual)
        if not expected.equals(actual):
          raise AssertionError(
              'Series not equal: \n%s\n%s\n' % (expected, actual))

      return check

    with beam.Pipeline() as p:
      one = p | 'One' >> beam.Create([one_series])
      two = p | 'Two' >> beam.Create([two_series])

      assert_that(
          one | 'PcollInPcollOut' >> transforms.DataframeTransform(
              lambda x: 3 * x, proxy=proxy),
          equal_to_series(three_series),
          label='CheckPcollInPcollOut')

      assert_that((one, two)
                  | 'TupleIn' >> transforms.DataframeTransform(
                      lambda x, y: (x + y), (proxy, proxy)),
                  equal_to_series(three_series),
                  label='CheckTupleIn')

      assert_that(
          dict(x=one, y=two)
          | 'DictIn' >> transforms.DataframeTransform(
              lambda x, y: (x + y), proxy=dict(x=proxy, y=proxy)),
          equal_to_series(three_series),
          label='CheckDictIn')

      double, triple = one | 'TupleOut' >> transforms.DataframeTransform(
              lambda x: (2*x, 3*x), proxy)
      assert_that(double, equal_to_series(two_series), 'CheckTupleOut0')
      assert_that(triple, equal_to_series(three_series), 'CheckTupleOut1')

      res = one | 'DictOut' >> transforms.DataframeTransform(
          lambda x: {'res': 3 * x}, proxy)
      assert_that(res['res'], equal_to_series(three_series), 'CheckDictOut')


if __name__ == '__main__':
  unittest.main()
