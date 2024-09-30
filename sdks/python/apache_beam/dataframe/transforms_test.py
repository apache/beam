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

import typing
import unittest
import warnings

import pandas as pd

import apache_beam as beam
from apache_beam import coders
from apache_beam import metrics
from apache_beam.dataframe import convert
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import transforms
from apache_beam.runners.portability.fn_api_runner import fn_runner
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


def check_correct(expected, actual):
  if actual is None:
    raise AssertionError('Empty frame but expected: \n\n%s' % (expected))
  if isinstance(expected, pd.core.generic.NDFrame):
    expected = expected.sort_index()
    actual = actual.sort_index()

    if isinstance(expected, pd.Series):
      pd.testing.assert_series_equal(expected, actual)
    elif isinstance(expected, pd.DataFrame):
      pd.testing.assert_frame_equal(expected, actual)
    else:
      raise ValueError(
          f"Expected value is a {type(expected)},"
          "not a Series or DataFrame.")
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
    'AnimalSpeed', [('Animal', str), ('Speed', int)])
coders.registry.register_coder(AnimalSpeed, coders.RowCoder)
Nested = typing.NamedTuple(
    'Nested', [('id', int), ('animal_speed', AnimalSpeed)])
coders.registry.register_coder(Nested, coders.RowCoder)


class TransformTest(unittest.TestCase):
  def run_scenario(self, input, func):
    expected = func(input)

    empty = input.iloc[0:0]
    input_placeholder = expressions.PlaceholderExpression(empty)
    input_deferred = frame_base.DeferredFrame.wrap(input_placeholder)
    actual_deferred = func(input_deferred)._expr.evaluate_at(
        expressions.Session({input_placeholder: input}))

    check_correct(expected, actual_deferred)

    with beam.Pipeline() as p:
      input_pcoll = p | beam.Create([input.iloc[::2], input.iloc[1::2]])
      input_df = convert.to_dataframe(input_pcoll, proxy=empty)
      output_df = func(input_df)

      output_proxy = output_df._expr.proxy()
      if isinstance(output_proxy, pd.core.generic.NDFrame):
        self.assertTrue(
            output_proxy.iloc[:0].equals(expected.iloc[:0]),
            (
                'Output proxy is incorrect:\n'
                f'Expected:\n{expected.iloc[:0]}\n\n'
                f'Actual:\n{output_proxy.iloc[:0]}'))
      else:
        self.assertEqual(type(output_proxy), type(expected))

      output_pcoll = convert.to_pcollection(output_df, yield_elements='pandas')

      assert_that(
          output_pcoll, lambda actual: check_correct(expected, concat(actual)))

  def test_identity(self):
    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self.run_scenario(df, lambda x: x)

  def test_groupby_sum_mean(self):
    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self.run_scenario(df, lambda df: df.groupby('Animal').sum())
    with expressions.allow_non_parallel_operations():
      self.run_scenario(df, lambda df: df.groupby('Animal').mean())
    self.run_scenario(
        df, lambda df: df.loc[df.Speed > 25].groupby('Animal').sum())

  def test_groupby_apply(self):
    df = pd.DataFrame({
        'group': ['a' if i % 5 == 0 or i % 3 == 0 else 'b' for i in range(100)],
        'foo': [None if i % 11 == 0 else i for i in range(100)],
        'bar': [None if i % 7 == 0 else 99 - i for i in range(100)],
        'baz': [None if i % 13 == 0 else i * 2 for i in range(100)],
    })

    def median_sum_fn(x):
      with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Mean of empty slice")
        return (x.foo + x.bar).median()

    describe = lambda df: df.describe()

    self.run_scenario(df, lambda df: df.groupby('group').foo.apply(describe))
    self.run_scenario(
        df, lambda df: df.groupby('group')[['foo', 'bar']].apply(describe))
    self.run_scenario(df, lambda df: df.groupby('group').apply(median_sum_fn))
    self.run_scenario(
        df,
        lambda df: df.set_index('group').foo.groupby(level=0).apply(describe))
    self.run_scenario(df, lambda df: df.groupby(level=0).apply(median_sum_fn))
    self.run_scenario(
        df, lambda df: df.groupby(lambda x: x % 3).apply(describe))

  def test_filter(self):
    df = pd.DataFrame({
        'Animal': ['Aardvark', 'Ant', 'Elephant', 'Zebra'],
        'Speed': [5, 2, 35, 40]
    })
    self.run_scenario(df, lambda df: df.filter(items=['Animal']))
    self.run_scenario(df, lambda df: df.filter(regex='Anim.*'))
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
      self.run_scenario(a, lambda a: a / (a.max() - a.min()))
      self.run_scenario(a, lambda a: a / (a.sum() - 1))

      # Tests scalar being used as an input to a downstream stage.
      df = pd.DataFrame({'key': ['a', 'a', 'b'], 'val': [1, 2, 6]})
      self.run_scenario(
          df, lambda df: df.groupby('key').sum().val / df.val.agg(sum))

  def test_getitem_projection(self):
    df = pd.DataFrame({
        'Animal': ['Aardvark', 'Ant', 'Elephant', 'Zebra'],
        'Speed': [5, 2, 35, 40],
        'Size': ['Small', 'Extra Small', 'Large', 'Medium']
    })
    self.run_scenario(df, lambda df: df[['Speed', 'Size']])

  def test_offset_elementwise(self):
    s = pd.Series(range(10)).astype(float)
    df = pd.DataFrame({'value': s, 'square': s * s, 'cube': s * s * s})
    # Only those values that are both squares and cubes will intersect.
    self.run_scenario(
        df,
        lambda df: df.set_index('square').value + df.set_index('cube').value)

  def test_batching_named_tuple_input(self):
    with beam.Pipeline() as p:
      result = (
          p | beam.Create([
              AnimalSpeed('Aardvark', 5),
              AnimalSpeed('Ant', 2),
              AnimalSpeed('Elephant', 35),
              AnimalSpeed('Zebra', 40)
          ]).with_output_types(AnimalSpeed)
          | transforms.DataframeTransform(lambda df: df.filter(regex='Anim.*')))

      assert_that(
          result,
          equal_to([('Aardvark', ), ('Ant', ), ('Elephant', ), ('Zebra', )]))

  def test_batching_beam_row_input(self):
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create([('Falcon', 380.), ('Falcon', 370.), ('Parrot', 24.),
                         ('Parrot', 26.)])
          | beam.Map(lambda tpl: beam.Row(Animal=tpl[0], Speed=tpl[1]))
          | transforms.DataframeTransform(
              lambda df: df.groupby('Animal').mean(), include_indexes=True))

      assert_that(result, equal_to([('Falcon', 375.), ('Parrot', 25.)]))

  def test_batching_beam_row_to_dataframe(self):
    with beam.Pipeline() as p:
      df = convert.to_dataframe(
          p
          | beam.Create([('Falcon', 380.), ('Falcon', 370.), ('Parrot', 24.), (
              'Parrot', 26.)])
          | beam.Map(lambda tpl: beam.Row(Animal=tpl[0], Speed=tpl[1])))

      result = convert.to_pcollection(
          df.groupby('Animal').mean(), include_indexes=True)

      assert_that(result, equal_to([('Falcon', 375.), ('Parrot', 25.)]))

  def test_batching_passthrough_nested_schema(self):
    with beam.Pipeline() as p:
      nested_schema_pc = (
          p | beam.Create([Nested(1, AnimalSpeed('Aardvark', 5))
                           ]).with_output_types(Nested))
      result = nested_schema_pc | transforms.DataframeTransform(  # pylint: disable=expression-not-assigned
          lambda df: df.filter(items=['animal_speed']))

      assert_that(result, equal_to([(('Aardvark', 5), )]))

  def test_batching_passthrough_nested_array(self):
    Array = typing.NamedTuple(
        'Array', [('id', int), ('business_numbers', typing.Sequence[int])])
    coders.registry.register_coder(Array, coders.RowCoder)

    with beam.Pipeline() as p:
      array_schema_pc = (p | beam.Create([Array(1, [7, 8, 9])]))
      result = array_schema_pc | transforms.DataframeTransform(  # pylint: disable=expression-not-assigned
            lambda df: df.filter(items=['business_numbers']))

      assert_that(result, equal_to([([7, 8, 9], )]))

  def test_unbatching_series(self):
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create([('Falcon', 380.), ('Falcon', 370.), ('Parrot', 24.),
                         ('Parrot', 26.)])
          | beam.Map(lambda tpl: beam.Row(Animal=tpl[0], Speed=tpl[1]))
          | transforms.DataframeTransform(lambda df: df.Animal))

      assert_that(result, equal_to(['Falcon', 'Falcon', 'Parrot', 'Parrot']))

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
              lambda x: 3 * x, proxy=proxy, yield_elements='pandas'),
          equal_to_series(three_series),
          label='CheckPcollInPcollOut')

      assert_that(
          (one, two)
          | 'TupleIn' >> transforms.DataframeTransform(
              lambda x, y: (x + y), (proxy, proxy), yield_elements='pandas'),
          equal_to_series(three_series),
          label='CheckTupleIn')

      assert_that(
          dict(x=one, y=two)
          | 'DictIn' >> transforms.DataframeTransform(
              lambda x,
              y: (x + y),
              proxy=dict(x=proxy, y=proxy),
              yield_elements='pandas'),
          equal_to_series(three_series),
          label='CheckDictIn')

      double, triple = one | 'TupleOut' >> transforms.DataframeTransform(
              lambda x: (2*x, 3*x), proxy, yield_elements='pandas')
      assert_that(double, equal_to_series(two_series), 'CheckTupleOut0')
      assert_that(triple, equal_to_series(three_series), 'CheckTupleOut1')

      res = one | 'DictOut' >> transforms.DataframeTransform(
          lambda x: {'res': 3 * x}, proxy, yield_elements='pandas')
      assert_that(res['res'], equal_to_series(three_series), 'CheckDictOut')

  def test_cat(self):
    # verify that cat works with a List[Series] since this is
    # missing from doctests
    df = pd.DataFrame({
        'one': ['A', 'B', 'C'],
        'two': ['BB', 'CC', 'A'],
        'three': ['CCC', 'AA', 'B'],
    })
    self.run_scenario(df, lambda df: df.two.str.cat([df.three], join='outer'))
    self.run_scenario(
        df, lambda df: df.one.str.cat([df.two, df.three], join='outer'))

  def test_repeat(self):
    # verify that repeat works with a Series since this is
    # missing from doctests
    df = pd.DataFrame({
        'strings': ['A', 'B', 'C', 'D', 'E'],
        'repeats': [3, 1, 4, 5, 2],
    })
    self.run_scenario(df, lambda df: df.strings.str.repeat(df.repeats))

  def test_rename(self):
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    self.run_scenario(
        df, lambda df: df.rename(columns={'B': 'C'}, index={
            0: 2, 2: 0
        }))

    with expressions.allow_non_parallel_operations():
      self.run_scenario(
          df,
          lambda df: df.rename(
              columns={'B': 'C'}, index={
                  0: 2, 2: 0
              }, errors='raise'))

  def test_dataframe_column_fillna_constant_as_value(self):
    df = pd.DataFrame({'A': (1, "NAN", 1), 'B': (1, 1, 1)})

    def column_fillna_constant_as_value_function(df):
      df['A'] = df['A'].fillna(0)
      return df

    self.run_scenario(
        df, lambda df: column_fillna_constant_as_value_function(df))


class FusionTest(unittest.TestCase):
  @staticmethod
  def fused_stages(p):
    return p.result.monitoring_metrics().query(
        metrics.MetricsFilter().with_name(
            fn_runner.FnApiRunner.NUM_FUSED_STAGES_COUNTER)
    )['counters'][0].result

  @staticmethod
  def create_animal_speed_input(p):
    return p | beam.Create([
        AnimalSpeed('Aardvark', 5),
        AnimalSpeed('Ant', 2),
        AnimalSpeed('Elephant', 35),
        AnimalSpeed('Zebra', 40)
    ],
                           reshuffle=False)

  def test_loc_filter(self):
    with beam.Pipeline() as p:
      _ = (
          self.create_animal_speed_input(p)
          | transforms.DataframeTransform(lambda df: df[df.Speed > 10]))
    self.assertEqual(self.fused_stages(p), 1)

  def test_column_manipulation(self):
    def set_column(df, name, s):
      df[name] = s
      return df

    with beam.Pipeline() as p:
      _ = (
          self.create_animal_speed_input(p)
          | transforms.DataframeTransform(
              lambda df: set_column(df, 'x', df.Speed + df.Animal.str.len())))
    self.assertEqual(self.fused_stages(p), 1)


class TransformPartsTest(unittest.TestCase):
  def test_rebatch(self):
    with beam.Pipeline() as p:
      sA = pd.Series(range(1000))
      sB = sA * sA
      pcA = p | 'CreatePCollA' >> beam.Create([('k0', sA[::3]),
                                               ('k1', sA[1::3]),
                                               ('k2', sA[2::3])])
      pcB = p | 'CreatePCollB' >> beam.Create([('k0', sB[::3]),
                                               ('k1', sB[1::3]),
                                               ('k2', sB[2::3])])
      input = {'A': pcA, 'B': pcB} | beam.CoGroupByKey()
      output = input | beam.ParDo(
          transforms._ReBatch(target_size=sA.memory_usage()))

      # There should be exactly two elements, as the target size will be
      # hit when 2/3 of pcA and 2/3 of pcB is seen, but not before.
      assert_that(output | beam.combiners.Count.Globally(), equal_to([2]))

      # Sanity check that we got all the right values.
      assert_that(
          output | beam.Map(lambda x: x['A'].sum())
          | 'SumA' >> beam.CombineGlobally(sum),
          equal_to([sA.sum()]),
          label='CheckValuesA')
      assert_that(
          output | beam.Map(lambda x: x['B'].sum())
          | 'SumB' >> beam.CombineGlobally(sum),
          equal_to([sB.sum()]),
          label='CheckValuesB')


if __name__ == '__main__':
  unittest.main()
