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

"""Tests for arrow_type_compatibility."""

import logging
import unittest
from collections.abc import Sequence
from typing import Any
from typing import NamedTuple
from typing import Optional

import pyarrow as pa
import pytest
from parameterized import parameterized
from parameterized import parameterized_class

from apache_beam.typehints import row_type
from apache_beam.typehints import typehints
from apache_beam.typehints.arrow_type_compatibility import arrow_schema_from_beam_schema
from apache_beam.typehints.arrow_type_compatibility import beam_schema_from_arrow_schema
from apache_beam.typehints.batch import BatchConverter
from apache_beam.typehints.batch_test import temp_seed
from apache_beam.typehints.schemas_test import get_test_beam_schemas_protos


@pytest.mark.uses_pyarrow
class ArrowTypeCompatibilityTest(unittest.TestCase):
  @parameterized.expand([(beam_schema, )
                         for beam_schema in get_test_beam_schemas_protos()])
  def test_beam_schema_survives_roundtrip(self, beam_schema):
    roundtripped = beam_schema_from_arrow_schema(
        arrow_schema_from_beam_schema(beam_schema))

    self.assertEqual(beam_schema, roundtripped)


@parameterized_class([
    {
        'batch_typehint': pa.Table,
        'element_typehint': row_type.RowTypeConstraint.from_fields([
            ('foo', Optional[int]),
            ('bar', Optional[float]),
            ('baz', Optional[str]),
        ]),
        'batch': pa.Table.from_pydict({
            'foo': pa.array(range(100), type=pa.int64()),
            'bar': pa.array([i / 100 for i in range(100)], type=pa.float64()),
            'baz': pa.array([str(i) for i in range(100)], type=pa.string()),
        }),
    },
    {
        'batch_typehint': pa.Table,
        'element_typehint': row_type.RowTypeConstraint.from_fields([
            ('foo', Optional[int]),
            (
                'nested',
                Optional[row_type.RowTypeConstraint.from_fields([
                    ("bar", Optional[float]),  # noqa: F821
                    ("baz", Optional[str]),  # noqa: F821
                ])]),
        ]),
        'batch': pa.Table.from_pydict({
            'foo': pa.array(range(100), type=pa.int64()),
            'nested': pa.array([
                None if i % 11 else {
                    'bar': i / 100, 'baz': str(i)
                } for i in range(100)
            ]),
        }),
    },
    {
        'batch_typehint': pa.Table,
        'element_typehint': row_type.RowTypeConstraint.from_fields([
            ('foo', Optional[int]),
            (
                'nested_list',
                Optional[Sequence[Optional[
                    row_type.RowTypeConstraint.from_fields([
                        ("bar", Optional[float]),  # noqa: F821
                        ("baz", Optional[str]),  # noqa: F821
                    ])]]]),
        ]),
        'batch': pa.Table.from_pydict({
            'foo': pa.array(range(100), type=pa.int64()),
            'nested_list': pa.array([[{
                'bar': i / 100, 'baz': str(i)
            }, None] if i % 13 else None for i in range(100)]),
        }),
    },
    {
        'batch_typehint': pa.Array,
        'element_typehint': int,
        'batch': pa.array(range(100), type=pa.int64()),
    },
    {
        'batch_typehint': pa.Array,
        'element_typehint': row_type.RowTypeConstraint.from_fields([
            ("bar", Optional[float]),  # noqa: F821
            ("baz", Optional[str]),  # noqa: F821
        ]),
        'batch': pa.array([{
            'bar': i / 100, 'baz': str(i)
        } if i % 7 else None for i in range(100)]),
    }
])
@pytest.mark.uses_pyarrow
class ArrowBatchConverterTest(unittest.TestCase):
  def create_batch_converter(self):
    return BatchConverter.from_typehints(
        element_type=self.element_typehint, batch_type=self.batch_typehint)

  def setUp(self):
    self.converter = self.create_batch_converter()
    self.normalized_batch_typehint = typehints.normalize(self.batch_typehint)
    self.normalized_element_typehint = typehints.normalize(
        self.element_typehint)

  def equality_check(self, left, right):
    if isinstance(left, pa.Array):
      self.assertTrue(left.equals(right))
    else:
      self.assertEqual(left, right)

  def test_typehint_validates(self):
    typehints.validate_composite_type_param(self.batch_typehint, '')
    typehints.validate_composite_type_param(self.element_typehint, '')

  def test_type_check(self):
    typehints.check_constraint(self.normalized_batch_typehint, self.batch)

  def test_type_check_element(self):
    for element in self.converter.explode_batch(self.batch):
      typehints.check_constraint(self.normalized_element_typehint, element)

  def test_explode_rebatch(self):
    exploded = list(self.converter.explode_batch(self.batch))
    rebatched = self.converter.produce_batch(exploded)

    typehints.check_constraint(self.normalized_batch_typehint, rebatched)
    self.equality_check(self.batch, rebatched)

  def test_estimate_byte_size_implemented(self):
    # Just verify that we can call byte size
    self.assertGreater(self.converter.estimate_byte_size(self.batch), 0)

  @parameterized.expand([
      (2, ),
      (3, ),
      (10, ),
  ])
  def test_estimate_byte_size_partitions(self, N):
    elements = list(self.converter.explode_batch(self.batch))

    # Split elements into N contiguous partitions, create a batch out of each
    batches = [
        self.converter.produce_batch(
            elements[len(elements) * i // N:len(elements) * (i + 1) // N])
        for i in range(N)
    ]

    # Some estimate_byte_size implementations use random samples,
    # set a seed temporarily to make this test deterministic
    with temp_seed(12345):
      partitioned_size_estimate = sum(
          self.converter.estimate_byte_size(batch) for batch in batches)
      size_estimate = self.converter.estimate_byte_size(self.batch)

    # Assert that size estimate for partitions is within 10% of size estimate
    # for the whole partition.
    self.assertLessEqual(
        abs(partitioned_size_estimate / size_estimate - 1), 0.1)

  @parameterized.expand([
      (2, ),
      (3, ),
      (10, ),
  ])
  def test_combine_batches(self, N):
    elements = list(self.converter.explode_batch(self.batch))

    # Split elements into N contiguous partitions, create a batch out of each
    batches = [
        self.converter.produce_batch(
            elements[len(elements) * i // N:len(elements) * (i + 1) // N])
        for i in range(N)
    ]

    # Combine the batches, output should be equivalent to the original batch
    combined = self.converter.combine_batches(batches)

    self.equality_check(self.batch, combined)

  def test_equals(self):
    self.assertTrue(self.converter == self.create_batch_converter())
    self.assertTrue(self.create_batch_converter() == self.converter)

  def test_hash(self):
    self.assertEqual(hash(self.create_batch_converter()), hash(self.converter))


class Address(NamedTuple):
  city: Optional[str]
  zip: Optional[int]


class Person(NamedTuple):
  address: Optional[Address]
  history: Optional[Sequence[Optional[Address]]]


@pytest.mark.uses_pyarrow
class ArrowBatchConverterNestedRowsTest(unittest.TestCase):
  BATCH = pa.Table.from_pydict({
      'address': [{
          'city': 'SF', 'zip': 1
      }, None],
      'history': [[{
          'city': 'NY', 'zip': 2
      }, None], None],
  })

  def test_explode_batch_builds_nested_rows(self):
    address_type = row_type.RowTypeConstraint.from_fields([
        ('city', Optional[str]),
        ('zip', Optional[int]),
    ])
    converter = BatchConverter.from_typehints(
        element_type=row_type.RowTypeConstraint.from_fields([
            ('address', Optional[address_type]),
            ('history', Optional[Sequence[Optional[address_type]]]),
        ]),
        batch_type=pa.Table)

    first, second = converter.explode_batch(self.BATCH)

    address = address_type.user_type
    self.assertIsInstance(first.address, address)
    self.assertEqual(first.address, address(city='SF', zip=1))
    self.assertEqual(first.history, [address(city='NY', zip=2), None])
    self.assertIsNone(second.address)
    self.assertIsNone(second.history)

  def test_explode_batch_builds_nested_user_types(self):
    converter = BatchConverter.from_typehints(
        element_type=Person, batch_type=pa.Table)

    first, second = converter.explode_batch(self.BATCH)

    self.assertEqual(
        first,
        Person(
            address=Address(city='SF', zip=1),
            history=[Address(city='NY', zip=2), None]))
    self.assertIsInstance(first.address, Address)
    self.assertEqual(second, Person(address=None, history=None))

  def test_explode_array_batch_builds_rows(self):
    converter = BatchConverter.from_typehints(
        element_type=Address, batch_type=pa.Array)

    exploded = list(converter.explode_batch(self.BATCH.column('address')))

    self.assertEqual(exploded, [Address(city='SF', zip=1), None])
    self.assertIsInstance(exploded[0], Address)

  @parameterized.expand([
      (
          'optional',
          Optional[Address],
          'address', [Address(city='SF', zip=1), None]),
      (
          'sequence',
          Optional[Sequence[Optional[Address]]],
          'history', [[Address(city='NY', zip=2), None], None]),
  ])
  def test_explode_array_batch_builds_nested_rows(
      self, _, element_type, column, expected):
    # from_typehints normalizes these hints to Beam type constraints.
    converter = BatchConverter.from_typehints(
        element_type=element_type, batch_type=pa.Array)

    exploded = list(converter.explode_batch(self.BATCH.column(column)))

    self.assertEqual(exploded, expected)


class ArrowBatchConverterErrorsTest(unittest.TestCase):
  @parameterized.expand([
      (
          pa.RecordBatch,
          row_type.RowTypeConstraint.from_fields([
              ("bar", Optional[float]),  # noqa: F821
              ("baz", Optional[str]),  # noqa: F821
          ]),
          r'batch type must be pa\.Table or pa\.Array',
      ),
      (
          pa.Table,
          Any,
          r'Element type .* must be compatible with Beam Schemas',
      ),
  ])
  def test_construction_errors(
      self, batch_typehint, element_typehint, error_regex):
    with self.assertRaisesRegex(TypeError, error_regex):
      BatchConverter.from_typehints(
          element_type=element_typehint, batch_type=batch_typehint)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
