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

"""Unit tests for the core python file."""
# pytype: skip-file

import logging
import unittest

import pytest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import FixedWindows


class PartitionTest(unittest.TestCase):
  def test_partition_boundedness(self):
    def partition_fn(val, num_partitions):
      return val % num_partitions

    class UnboundedDoFn(beam.DoFn):
      @beam.DoFn.unbounded_per_element()
      def process(self, element):
        yield element

    with beam.testing.test_pipeline.TestPipeline() as p:
      source = p | beam.Create([1, 2, 3, 4, 5])
      p1, p2, p3 = source | "bounded" >> beam.Partition(partition_fn, 3)

      self.assertEqual(source.is_bounded, True)
      self.assertEqual(p1.is_bounded, True)
      self.assertEqual(p2.is_bounded, True)
      self.assertEqual(p3.is_bounded, True)

      unbounded = source | beam.ParDo(UnboundedDoFn())
      p4, p5, p6 = unbounded | "unbounded" >> beam.Partition(partition_fn, 3)

      self.assertEqual(unbounded.is_bounded, False)
      self.assertEqual(p4.is_bounded, False)
      self.assertEqual(p5.is_bounded, False)
      self.assertEqual(p6.is_bounded, False)


class FlattenTest(unittest.TestCase):
  def test_flatten_identical_windows(self):
    with beam.testing.test_pipeline.TestPipeline() as p:
      source1 = p | "c1" >> beam.Create(
          [1, 2, 3, 4, 5]) | "w1" >> beam.WindowInto(FixedWindows(100))
      source2 = p | "c2" >> beam.Create([6, 7, 8]) | "w2" >> beam.WindowInto(
          FixedWindows(100))
      source3 = p | "c3" >> beam.Create([9, 10]) | "w3" >> beam.WindowInto(
          FixedWindows(100))
      out = (source1, source2, source3) | "flatten" >> beam.Flatten()
      assert_that(out, equal_to([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))

  def test_flatten_no_windows(self):
    with beam.testing.test_pipeline.TestPipeline() as p:
      source1 = p | "c1" >> beam.Create([1, 2, 3, 4, 5])
      source2 = p | "c2" >> beam.Create([6, 7, 8])
      source3 = p | "c3" >> beam.Create([9, 10])
      out = (source1, source2, source3) | "flatten" >> beam.Flatten()
      assert_that(out, equal_to([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))

  def test_flatten_mismatched_windows(self):
    with beam.testing.test_pipeline.TestPipeline() as p:
      source1 = p | "c1" >> beam.Create(
          [1, 2, 3, 4, 5]) | "w1" >> beam.WindowInto(FixedWindows(25))
      source2 = p | "c2" >> beam.Create([6, 7, 8]) | "w2" >> beam.WindowInto(
          FixedWindows(100))
      source3 = p | "c3" >> beam.Create([9, 10]) | "w3" >> beam.WindowInto(
          FixedWindows(100))
      _ = (source1, source2, source3) | "flatten" >> beam.Flatten()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
