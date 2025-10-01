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

import unittest
from typing import Any
from typing import TypeVar

import apache_beam as beam
from apache_beam.transforms.window import TimestampedValue

T = TypeVar("T")


def ConvertToTimestampedValue(plant: dict[str, Any]) -> TimestampedValue[str]:
  return TimestampedValue[str](plant["name"], plant["season"])


def ConvertToTimestampedValue_1(plant: dict[str, Any]) -> TimestampedValue:
  return TimestampedValue(plant["name"], plant["season"])


def ConvertToTimestampedValue_2(
    plant: dict[str, Any]) -> TimestampedValue[list[str]]:
  return TimestampedValue[list[str]](plant["name"], plant["season"])


def ConvertToTimestampedValue_3(plant: dict[str, Any]) -> TimestampedValue[T]:
  return TimestampedValue[T](plant["name"], plant["season"])


class TypeCheckTimestampedValueTestCase(unittest.TestCase):
  def setUp(self):
    self.opts = beam.options.pipeline_options.PipelineOptions(
        runtime_type_check=True)
    self.data = [
        {
            "name": "Strawberry", "season": 1585699200
        },  # April, 2020
    ]
    self.data_1 = [
        {
            "name": 1234, "season": 1585699200
        },  # April, 2020
    ]
    self.data_2 = [
        {
            "name": ["abc", "cde"], "season": 1585699200
        },  # April, 2020
    ]
    self.data_3 = [
        {
            "name": [123, "cde"], "season": 1585699200
        },  # April, 2020
    ]

  def test_pcoll_default_hints(self):
    for fn in (ConvertToTimestampedValue, ConvertToTimestampedValue_1):
      pc = beam.Map(fn)
      ht = pc.default_type_hints()
      assert len(ht) == 3
      assert ht.output_types[0][0]

  def test_pcoll_with_output_hints(self):
    pc = beam.Map(ConvertToTimestampedValue).with_output_types(str)
    ht = pc.get_type_hints()
    assert len(ht) == 3
    assert ht.output_types[0][0] == str

  def test_opts_with_check(self):
    with beam.Pipeline(options=self.opts) as p:
      _ = (
          p
          | "Garden plants" >> beam.Create(self.data)
          | "With timestamps" >> beam.Map(ConvertToTimestampedValue)
          | beam.Map(print))

  def test_opts_with_check_list_str(self):
    with beam.Pipeline(options=self.opts) as p:
      _ = (
          p
          | "Garden plants" >> beam.Create(self.data_2)
          | "With timestamps" >> beam.Map(ConvertToTimestampedValue_2)
          | beam.Map(print))

  def test_opts_with_check_wrong_data(self):
    with self.assertRaises(Exception):
      with beam.Pipeline(options=self.opts) as p:
        _ = (
            p
            | "Garden plants" >> beam.Create(self.data_1)
            | "With timestamps" >> beam.Map(ConvertToTimestampedValue)
            | beam.Map(print))

  def test_opts_with_check_wrong_data_list_str(self):
    with self.assertRaises(Exception):
      with beam.Pipeline(options=self.opts) as p:
        _ = (
            p
            | "Garden plants" >> beam.Create(self.data_1)
            | "With timestamps" >> beam.Map(ConvertToTimestampedValue_2)
            | beam.Map(print))

    with self.assertRaises(Exception):
      with beam.Pipeline(options=self.opts) as p:
        _ = (
            p
            | "Garden plants" >> beam.Create(self.data_3)
            | "With timestamps" >> beam.Map(ConvertToTimestampedValue_2)
            | beam.Map(print))

  def test_opts_with_check_typevar(self):
    with self.assertRaises(Exception):
      with beam.Pipeline(options=self.opts) as p:
        _ = (
            p
            | "Garden plants" >> beam.Create(self.data_2)
            | "With timestamps" >> beam.Map(ConvertToTimestampedValue_3)
            | beam.Map(print))


if __name__ == '__main__':
  unittest.main()
