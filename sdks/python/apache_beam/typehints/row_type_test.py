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

"""Unit tests for the Beam Row typing functionality."""

import typing
import unittest
from dataclasses import dataclass

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.typehints import row_type


class RowTypeTest(unittest.TestCase):
  @staticmethod
  def _check_key_type_and_count(x) -> int:
    key_type = type(x[0])
    if not row_type._user_type_is_generated(key_type):
      raise RuntimeError("Expect type after GBK to be generated user type")

    return len(x[1])

  def test_group_by_key_namedtuple(self):
    MyNamedTuple = typing.NamedTuple(
        "MyNamedTuple", [("id", int), ("name", str)])

    beam.coders.typecoders.registry.register_coder(
        MyNamedTuple, beam.coders.RowCoder)

    def generate(num: int):
      for i in range(100):
        yield (MyNamedTuple(i, 'a'), num)

    pipeline = TestPipeline(is_integration_test=False)

    with pipeline as p:
      result = (
          p
          | 'Create' >> beam.Create([i for i in range(10)])
          | 'Generate' >> beam.ParDo(generate).with_output_types(
              tuple[MyNamedTuple, int])
          | 'GBK' >> beam.GroupByKey()
          | 'Count Elements' >> beam.Map(self._check_key_type_and_count))
      assert_that(result, equal_to([10] * 100))

  def test_group_by_key_dataclass(self):
    @dataclass
    class MyDataClass:
      id: int
      name: str

    beam.coders.typecoders.registry.register_coder(
        MyDataClass, beam.coders.RowCoder)

    def generate(num: int):
      for i in range(100):
        yield (MyDataClass(i, 'a'), num)

    pipeline = TestPipeline(is_integration_test=False)

    with pipeline as p:
      result = (
          p
          | 'Create' >> beam.Create([i for i in range(10)])
          | 'Generate' >> beam.ParDo(generate).with_output_types(
              tuple[MyDataClass, int])
          | 'GBK' >> beam.GroupByKey()
          | 'Count Elements' >> beam.Map(self._check_key_type_and_count))
      assert_that(result, equal_to([10] * 100))


if __name__ == '__main__':
  unittest.main()
