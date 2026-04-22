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

"""Unit tests for the PValue and PCollection classes."""

# pytype: skip-file

import copy
import unittest

import apache_beam as beam
from apache_beam.pvalue import AsSingleton
from apache_beam.pvalue import PCollection
from apache_beam.pvalue import PValue
from apache_beam.pvalue import Row
from apache_beam.pvalue import TaggedOutput
from apache_beam.testing.test_pipeline import TestPipeline


class PValueTest(unittest.TestCase):
  def test_pvalue_expected_arguments(self):
    pipeline = TestPipeline()
    value = PValue(pipeline)
    self.assertEqual(pipeline, value.pipeline)

  def test_assingleton_multi_element(self):
    with self.assertRaisesRegex(
        ValueError,
        'PCollection of size 2 with more than one element accessed as a '
        'singleton view. First two elements encountered are \"1\", \"2\".'):
      AsSingleton._from_runtime_iterable([1, 2], {})


class TaggedValueTest(unittest.TestCase):
  def test_passed_tuple_as_tag(self):
    with self.assertRaisesRegex(
        TypeError,
        r'Attempting to create a TaggedOutput with non-string tag \(1, 2, 3\)'):
      TaggedOutput((1, 2, 3), 'value')


class PCollectionSideOutputsTest(unittest.TestCase):
  def test_with_side_outputs_returns_copy_and_preserves_original(self):
    pipeline = beam.Pipeline()
    pcoll = PCollection(pipeline)
    dropped = PCollection(pipeline)

    result = pcoll.with_side_outputs(dropped=dropped)
    copied = copy.copy(result)

    self.assertIsNot(result, pcoll)
    self.assertIsNone(pcoll._side_outputs)
    self.assertEqual({'dropped': dropped}, result._side_outputs)
    self.assertEqual({'dropped': dropped}, copied._side_outputs)

  def test_side_outputs_attribute_and_index_access(self):
    pipeline = beam.Pipeline()
    dropped = PCollection(pipeline)
    kept = PCollection(pipeline)
    pcoll = PCollection(pipeline).with_side_outputs(dropped=dropped, kept=kept)

    self.assertIs(pcoll.side_outputs.dropped, dropped)
    self.assertIs(pcoll.side_outputs['kept'], kept)
    self.assertEqual(['dropped', 'kept'], sorted(pcoll.side_outputs))
    self.assertEqual(2, len(pcoll.side_outputs))
    self.assertIn('dropped', pcoll.side_outputs)

  def test_missing_side_output_lists_available_tags(self):
    pipeline = beam.Pipeline()
    dropped = PCollection(pipeline)
    kept = PCollection(pipeline)
    pcoll = PCollection(pipeline).with_side_outputs(dropped=dropped, kept=kept)

    with self.assertRaisesRegex(
        AttributeError,
        r"No side output named 'missing'\. Available: \['dropped', 'kept'\]"):
      _ = pcoll.side_outputs.missing

  def test_with_side_outputs_rejects_non_pcollection(self):
    pcoll = PCollection(beam.Pipeline())

    with self.assertRaisesRegex(TypeError,
                                r"Side output 'dropped' must be a PCollection"):
      pcoll.with_side_outputs(dropped='not a PCollection')

  def test_with_side_outputs_rejects_different_pipeline(self):
    pcoll = PCollection(beam.Pipeline())
    dropped = PCollection(beam.Pipeline())

    with self.assertRaisesRegex(
        ValueError, r"Side output 'dropped' must belong to the same pipeline"):
      pcoll.with_side_outputs(dropped=dropped)

  def test_side_outputs_empty_container_behavior(self):
    pcoll = PCollection(beam.Pipeline())

    self.assertEqual([], list(pcoll.side_outputs))
    self.assertEqual(0, len(pcoll.side_outputs))
    self.assertNotIn('missing', pcoll.side_outputs)
    with self.assertRaisesRegex(
        AttributeError, r"No side output named 'missing'\. Available: \[\]"):
      _ = pcoll.side_outputs.missing

  def test_with_side_outputs_second_call_replaces_existing_side_outputs(self):
    pipeline = beam.Pipeline()
    dropped = PCollection(pipeline)
    kept = PCollection(pipeline)
    first = PCollection(pipeline).with_side_outputs(dropped=dropped)

    second = first.with_side_outputs(kept=kept)

    self.assertEqual({'dropped': dropped}, first._side_outputs)
    self.assertEqual({'kept': kept}, second._side_outputs)
    with self.assertRaises(AttributeError):
      _ = second.side_outputs.dropped


class RowTest(unittest.TestCase):
  def test_row_eq(self):
    row = Row(a=1, b=2)
    same = Row(a=1, b=2)
    self.assertEqual(row, same)

  def test_trailing_column_row_neq(self):
    row = Row(a=1, b=2)
    trail = Row(a=1, b=2, c=3)
    self.assertNotEqual(row, trail)

  def test_row_comparison_respects_element_order(self):
    row = Row(a=1, b=2)
    different = Row(b=2, a=1)
    self.assertNotEqual(row, different)


if __name__ == '__main__':
  unittest.main()
