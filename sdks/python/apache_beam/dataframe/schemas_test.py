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

"""Tests for schemas."""

# pytype: skip-file

from __future__ import absolute_import

import unittest
import typing

import future.tests.base  # pylint: disable=unused-import
# patches unittest.testcase to be python3 compatible
import pandas as pd
import numpy as np
from past.builtins import unicode

import apache_beam as beam
from apache_beam.coders import RowCoder
from apache_beam.coders.typecoders import registry as coders_registry
from apache_beam.dataframe import schemas
from apache_beam.dataframe import transforms
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from parameterized import parameterized

Simple = typing.NamedTuple(
    'Simple', [('name', unicode), ('id', int), ('height', float)])
coders_registry.register_coder(Simple, RowCoder)
Animal = typing.NamedTuple(
    'Animal', [('animal', unicode), ('max_speed', typing.Optional[float])])
coders_registry.register_coder(Animal, RowCoder)


def matches_df(expected):
  def check_df_pcoll_equal(actual):
    actual = pd.concat(actual)
    sorted_actual = actual.sort_values(by=list(actual.columns)).reset_index(
        drop=True)
    sorted_expected = expected.sort_values(
        by=list(expected.columns)).reset_index(drop=True)
    if not sorted_actual.equals(sorted_expected):
      raise AssertionError(
          'Dataframes not equal: \n\nActual:\n%s\n\nExpected:\n%s' %
          (sorted_actual, sorted_expected))

  return check_df_pcoll_equal


# Test data for all supported types that can be easily tested.
# Excludes bytes because it's difficult to create a series and dataframe bytes
# dtype. For example:
#   pd.Series([b'abc'], dtype=bytes).dtype != 'S'
#   pd.Series([b'abc'], dtype=bytes).astype(bytes).dtype == 'S'
COLUMNS = [
    ([375, 24, 0, 10, 16], np.int32, 'i32'),
    ([375, 24, 0, 10, 16], np.int64, 'i64'),
    ([375, 24, None, 10, 16], pd.Int32Dtype(), 'i32_nullable'),
    ([375, 24, None, 10, 16], pd.Int64Dtype(), 'i64_nullable'),
    ([375., 24., None, 10., 16.], np.float64, 'f64'),
    ([375., 24., None, 10., 16.], np.float32, 'f32'),
    ([True, False, True, True, False], np.bool, 'bool'),
    (['Falcon', 'Ostrich', None, 3.14, 0], np.object, 'any'),
]

if schemas.PD_MAJOR >= 1:
  COLUMNS.extend([
      ([True, False, True, None, False], pd.BooleanDtype(), 'bool_nullable'),
      (['Falcon', 'Ostrich', None, 'Aardvark', 'Elephant'],
       pd.StringDtype(),
       'strdtype'),
  ])

NICE_TYPES_DF = pd.DataFrame({
    name: pd.Series(arr, dtype=dtype, name=name)
    for arr,
    dtype,
    name in COLUMNS
})
NICE_TYPES_PROXY = NICE_TYPES_DF[:0]

SERIES_TESTS = [(pd.Series(arr, dtype=dtype, name=name), arr) for arr,
                dtype,
                name in COLUMNS]

DF_RESULT = list(zip(*(arr for arr, _, _ in COLUMNS)))
DF_TESTS = [(
    NICE_TYPES_DF
    if i == 0 else NICE_TYPES_DF.set_index([name
                                            for _, _, name in COLUMNS[:i]]),
    DF_RESULT) for i in range(len(COLUMNS) + 1)]


class SchemasTest(unittest.TestCase):
  def test_simple_df(self):
    expected = pd.DataFrame({
        'name': list(unicode(i) for i in range(5)),
        'id': list(range(5)),
        'height': list(float(i) for i in range(5))
    },
                            columns=['name', 'id', 'height'])

    with TestPipeline() as p:
      res = (
          p
          | beam.Create([
              Simple(name=unicode(i), id=i, height=float(i)) for i in range(5)
          ])
          | schemas.BatchRowsAsDataFrame(min_batch_size=10, max_batch_size=10))
      assert_that(res, matches_df(expected))

  def test_generate_proxy(self):
    expected = pd.DataFrame({
        'animal': pd.Series(
            dtype=np.object if schemas.PD_MAJOR < 1 else pd.StringDtype()),
        'max_speed': pd.Series(dtype=np.float64)
    })

    self.assertTrue(schemas.generate_proxy(Animal).equals(expected))

  def test_nice_types_proxy_roundtrip(self):
    roundtripped = schemas.generate_proxy(
        schemas.element_type_from_proxy(NICE_TYPES_PROXY))
    self.assertTrue(roundtripped.equals(NICE_TYPES_PROXY))

  # TODO
  @unittest.skip
  def test_bytes_proxy_roundtrip(self):
    proxy = pd.DataFrame({'bytes': []})
    proxy.bytes = proxy.bytes.astype(bytes)

    roundtripped = schemas.generate_proxy(
        schemas.element_type_from_proxy(proxy))
    self.assertTrue(roundtripped.equals(proxy))

  def test_batch_with_df_transform(self):
    with TestPipeline() as p:
      res = (
          p
          | beam.Create([
              Animal('Falcon', 380.0),
              Animal('Falcon', 370.0),
              Animal('Parrot', 24.0),
              Animal('Parrot', 26.0)
          ])
          | schemas.BatchRowsAsDataFrame()
          | transforms.DataframeTransform(
              lambda df: df.groupby('animal').mean(),
              # TODO: Generate proxy in this case as well
              proxy=schemas.generate_proxy(Animal)))
      assert_that(res, equal_to([('Falcon', 375.), ('Parrot', 25.)]))

  @parameterized.expand(SERIES_TESTS + DF_TESTS)
  def test_unbatch(self, df_or_series, rows):
    proxy = df_or_series[:0]

    with TestPipeline() as p:
      res = (
          p | beam.Create([df_or_series[::2], df_or_series[1::2]])
          | schemas.UnbatchPandas(proxy))

      assert_that(res, equal_to(rows))


if __name__ == '__main__':
  unittest.main()
