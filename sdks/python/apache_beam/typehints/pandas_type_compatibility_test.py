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

"""Unit tests for pandas batched type converters."""

import unittest
from typing import Any
from typing import Optional

import numpy as np
import pandas as pd
from parameterized import parameterized
from parameterized import parameterized_class

from apache_beam.typehints import row_type
from apache_beam.typehints import typehints
from apache_beam.typehints.batch import BatchConverter


@parameterized_class([
    {
        'batch_typehint': pd.DataFrame,
        'element_typehint': row_type.RowTypeConstraint.from_fields([
            ('foo', int),
            ('bar', float),
            ('baz', str),
        ]),
        'match_index': False,
        'batch': pd.DataFrame({
            'foo': pd.Series(range(100), dtype='int64'),
            'bar': pd.Series([i / 100 for i in range(100)], dtype='float64'),
            'baz': pd.Series([str(i) for i in range(100)],
                             dtype=pd.StringDtype()),
        }),
    },
    {
        'batch_typehint': pd.DataFrame,
        'element_typehint': row_type.RowTypeConstraint.from_fields(
            [
                ('an_index', int),
                ('foo', int),
                ('bar', float),
                ('baz', str),
            ],
            field_options={'an_index': [('beam:dataframe:index', None)]},
        ),
        'match_index': True,
        'batch': pd.DataFrame({
            'foo': pd.Series(range(100), dtype='int64'),
            'bar': pd.Series([i / 100 for i in range(100)], dtype='float64'),
            'baz': pd.Series([str(i) for i in range(100)],
                             dtype=pd.StringDtype()),
        }).set_index(pd.Index(range(123, 223), dtype='int64', name='an_index')),
    },
    {
        'batch_typehint': pd.DataFrame,
        'element_typehint': row_type.RowTypeConstraint.from_fields(
            [
                ('an_index', int),
                ('another_index', int),
                ('foo', int),
                ('bar', float),
                ('baz', str),
            ],
            field_options={
                'an_index': [('beam:dataframe:index', None)],
                'another_index': [('beam:dataframe:index', None)],
            }),
        'match_index': True,
        'batch': pd.DataFrame({
            'foo': pd.Series(range(100), dtype='int64'),
            'bar': pd.Series([i / 100 for i in range(100)], dtype='float64'),
            'baz': pd.Series([str(i) for i in range(100)],
                             dtype=pd.StringDtype()),
        }).set_index([
            pd.Index(range(123, 223), dtype='int64', name='an_index'),
            pd.Index(range(475, 575), dtype='int64', name='another_index'),
        ]),
    },
    {
        'batch_typehint': pd.Series,
        'element_typehint': int,
        'match_index': False,
        'batch': pd.Series(range(500)),
    },
    {
        'batch_typehint': pd.Series,
        'element_typehint': str,
        'batch': pd.Series(['foo', 'bar', 'baz', 'def', 'ghi', 'abc'] * 10,
                           dtype=pd.StringDtype()),
    },
    {
        'batch_typehint': pd.Series,
        'element_typehint': Optional[np.int64],
        'batch': pd.Series((i if i % 11 else None for i in range(500)),
                           dtype=pd.Int64Dtype()),
    },
    {
        'batch_typehint': pd.Series,
        'element_typehint': Optional[str],
        'batch': pd.Series(['foo', None, 'bar', 'baz', None, 'def', 'ghi'] * 10,
                           dtype=pd.StringDtype()),
    },
])
class PandasBatchConverterTest(unittest.TestCase):
  def create_batch_converter(self):
    return BatchConverter.from_typehints(
        element_type=self.element_typehint, batch_type=self.batch_typehint)

  def setUp(self):
    self.converter = self.create_batch_converter()
    self.normalized_batch_typehint = typehints.normalize(self.batch_typehint)
    self.normalized_element_typehint = typehints.normalize(
        self.element_typehint)

  def equality_check(self, left, right):
    if isinstance(left, pd.DataFrame):
      if self.match_index:
        pd.testing.assert_frame_equal(left.sort_index(), right.sort_index())
      else:
        pd.testing.assert_frame_equal(
            left.sort_values(by=list(left.columns)).reset_index(drop=True),
            right.sort_values(by=list(right.columns)).reset_index(drop=True))
    elif isinstance(left, pd.Series):
      pd.testing.assert_series_equal(
          left.sort_values().reset_index(drop=True),
          right.sort_values().reset_index(drop=True))
    else:
      raise TypeError(f"Encountered unexpected type, left is a {type(left)!r}")

  def test_typehint_validates(self):
    typehints.validate_composite_type_param(self.batch_typehint, '')
    typehints.validate_composite_type_param(self.element_typehint, '')

  def test_type_check_batch(self):
    typehints.check_constraint(self.normalized_batch_typehint, self.batch)

  def test_type_check_element(self):
    for element in self.converter.explode_batch(self.batch):
      typehints.check_constraint(self.normalized_element_typehint, element)

  def test_explode_rebatch(self):
    exploded = list(self.converter.explode_batch(self.batch))
    rebatched = self.converter.produce_batch(exploded)

    typehints.check_constraint(self.normalized_batch_typehint, rebatched)
    self.equality_check(self.batch, rebatched)

  def _split_batch_into_n_partitions(self, N):
    elements = list(self.converter.explode_batch(self.batch))

    # Split elements into N contiguous partitions
    element_batches = [
        elements[len(elements) * i // N:len(elements) * (i + 1) // N]
        for i in range(N)
    ]

    lengths = [len(element_batch) for element_batch in element_batches]
    batches = [
        self.converter.produce_batch(element_batch)
        for element_batch in element_batches
    ]

    return batches, lengths

  @parameterized.expand([
      (2, ),
      (3, ),
      (10, ),
  ])
  def test_combine_batches(self, N):
    batches, _ = self._split_batch_into_n_partitions(N)

    # Combine the batches, output should be equivalent to the original batch
    combined = self.converter.combine_batches(batches)

    self.equality_check(self.batch, combined)

  @parameterized.expand([
      (2, ),
      (3, ),
      (10, ),
  ])
  def test_get_length(self, N):
    batches, lengths = self._split_batch_into_n_partitions(N)

    for batch, expected_length in zip(batches, lengths):
      self.assertEqual(self.converter.get_length(batch), expected_length)

  def test_equals(self):
    self.assertTrue(self.converter == self.create_batch_converter())
    self.assertTrue(self.create_batch_converter() == self.converter)

  def test_hash(self):
    self.assertEqual(hash(self.create_batch_converter()), hash(self.converter))


class PandasBatchConverterErrorsTest(unittest.TestCase):
  @parameterized.expand([
      (
          Any,
          row_type.RowTypeConstraint.from_fields([
              ("bar", Optional[float]),  # noqa: F821
              ("baz", Optional[str]),  # noqa: F821
          ]),
          r'batch type must be pd\.Series or pd\.DataFrame',
      ),
      (
          pd.DataFrame,
          Any,
          r'Element type must be compatible with Beam Schemas',
      ),
  ])
  def test_construction_errors(
      self, batch_typehint, element_typehint, error_regex):
    with self.assertRaisesRegex(TypeError, error_regex):
      BatchConverter.from_typehints(
          element_type=element_typehint, batch_type=batch_typehint)


if __name__ == '__main__':
  unittest.main()
