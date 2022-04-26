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

import unittest

import pandas as pd

import apache_beam as beam
from apache_beam.dataframe import convert
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


def equal_to_unordered_series(expected):
  def check(actual):
    actual = pd.concat(actual)
    if sorted(expected) != sorted(actual):
      raise AssertionError('Series not equal: \n%s\n%s\n' % (expected, actual))

  return check


class ConvertTest(unittest.TestCase):
  def test_convert_yield_pandas(self):
    with beam.Pipeline() as p:
      a = pd.Series([1, 2, 3])
      b = pd.Series([100, 200, 300])

      pc_a = p | 'A' >> beam.Create([a])
      pc_b = p | 'B' >> beam.Create([b])

      df_a = convert.to_dataframe(pc_a, proxy=a[:0])
      df_b = convert.to_dataframe(pc_b, proxy=b[:0])

      df_2a = 2 * df_a
      df_3a = 3 * df_a
      df_ab = df_a * df_b

      # Converting multiple results at a time can be more efficient.
      pc_2a, pc_ab = convert.to_pcollection(df_2a, df_ab,
                                            yield_elements='pandas')
      # But separate conversions can be done as well.
      pc_3a = convert.to_pcollection(df_3a, yield_elements='pandas')

      assert_that(pc_2a, equal_to_unordered_series(2 * a), label='Check2a')
      assert_that(pc_3a, equal_to_unordered_series(3 * a), label='Check3a')
      assert_that(pc_ab, equal_to_unordered_series(a * b), label='Checkab')

  def test_convert(self):
    with beam.Pipeline() as p:
      a = pd.Series([1, 2, 3])
      b = pd.Series([100, 200, 300])

      pc_a = p | 'A' >> beam.Create(a)
      pc_b = p | 'B' >> beam.Create(b)

      df_a = convert.to_dataframe(pc_a)
      df_b = convert.to_dataframe(pc_b)

      df_2a = 2 * df_a
      df_3a = 3 * df_a
      df_ab = df_a * df_b

      # Converting multiple results at a time can be more efficient.
      pc_2a, pc_ab = convert.to_pcollection(df_2a, df_ab)
      # But separate conversions can be done as well.
      pc_3a = convert.to_pcollection(df_3a)

      assert_that(pc_2a, equal_to(list(2 * a)), label='Check2a')
      assert_that(pc_3a, equal_to(list(3 * a)), label='Check3a')
      assert_that(pc_ab, equal_to(list(a * b)), label='Checkab')

  def test_convert_with_none(self):
    # Ensure the logical Any type allows (nullable) None, see BEAM-12587.
    df = pd.DataFrame({'A': ['str', 10, None], 'B': [None, 'str', 20]})
    with beam.Pipeline() as p:
      res = convert.to_pcollection(df, pipeline=p) | beam.Map(tuple)
      assert_that(res, equal_to([(row.A, row.B) for _, row in df.iterrows()]))

  def test_convert_scalar(self):
    with beam.Pipeline() as p:
      pc = p | 'A' >> beam.Create([1, 2, 3])
      s = convert.to_dataframe(pc)
      pc_sum = convert.to_pcollection(s.sum())
      assert_that(pc_sum, equal_to([6]))

  def test_convert_non_deferred(self):
    with beam.Pipeline() as p:
      s1 = pd.Series([1, 2, 3])
      s2 = convert.to_dataframe(p | beam.Create([100, 200, 300]))

      pc1, pc2 = convert.to_pcollection(s1, s2, pipeline=p)
      assert_that(pc1, equal_to([1, 2, 3]), label='CheckNonDeferred')
      assert_that(pc2, equal_to([100, 200, 300]), label='CheckDeferred')

  def test_convert_memoization(self):
    with beam.Pipeline() as p:
      a = pd.Series([1, 2, 3])
      b = pd.Series([100, 200, 300])

      pc_a = p | 'A' >> beam.Create([a])
      pc_b = p | 'B' >> beam.Create([b])

      df_a = convert.to_dataframe(pc_a, proxy=a[:0])
      df_b = convert.to_dataframe(pc_b, proxy=b[:0])

      df_2a = 2 * df_a
      df_3a = 3 * df_a
      df_ab = df_a * df_b

      # Two calls to to_pcollection with the same Dataframe should produce the
      # same PCollection(s)
      pc_2a_, pc_ab_ = convert.to_pcollection(df_2a, df_ab)
      pc_3a, pc_2a, pc_ab = convert.to_pcollection(df_3a, df_2a, df_ab)

      self.assertIs(pc_2a, pc_2a_)
      self.assertIs(pc_ab, pc_ab_)
      self.assertIsNot(pc_3a, pc_2a)
      self.assertIsNot(pc_3a, pc_ab)

      # The same conversions without the unbatching transform should also cache
      # PCollections
      pc_2a_pandas_, pc_ab_pandas_ = convert.to_pcollection(df_2a, df_ab,
                                            yield_elements='pandas')
      pc_3a_pandas, pc_2a_pandas, pc_ab_pandas = convert.to_pcollection(df_3a,
                                                                        df_2a,
                                                                        df_ab,
                                                   yield_elements='pandas')

      self.assertIs(pc_2a_pandas, pc_2a_pandas_)
      self.assertIs(pc_ab_pandas, pc_ab_pandas_)
      self.assertIsNot(pc_3a_pandas, pc_2a_pandas)
      self.assertIsNot(pc_3a_pandas, pc_ab_pandas)

      # .. but the cached PCollections should be different
      self.assertIsNot(pc_2a_pandas, pc_2a)
      self.assertIsNot(pc_ab_pandas, pc_ab)
      self.assertIsNot(pc_3a_pandas, pc_3a)

  def test_convert_memoization_clears_cache(self):
    # This test re-runs the other memoization test, and makes sure that the
    # cache is cleaned up with the pipeline. Otherwise there would be concerns
    # of it growing without bound.

    import gc

    # Make sure cache is clear
    gc.collect()
    self.assertEqual(len(convert.TO_PCOLLECTION_CACHE), 0)

    # Disable GC so it doesn't run pre-emptively, confounding assertions about
    # cache size
    gc.disable()

    # Also disable logging, as some implementations may artificially extend
    # the life of objects.
    import logging
    logging.disable(logging.INFO)

    try:
      self.test_convert_memoization()
      self.assertEqual(len(convert.TO_PCOLLECTION_CACHE), 3)

      gc.collect()

      # PCollections should be removed from cache after pipelines go out of
      # scope and are GC'd
      self.assertEqual(len(convert.TO_PCOLLECTION_CACHE), 0)
    finally:
      # Always re-enable GC and logging
      gc.enable()
      logging.disable(logging.NOTSET)


if __name__ == '__main__':
  unittest.main()
