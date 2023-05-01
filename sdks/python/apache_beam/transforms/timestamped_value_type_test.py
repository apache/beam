from apache_beam.typehints import Dict, Any, List
from apache_beam.typehints.decorators import TypeCheckError
from apache_beam.transforms.window import TimestampedValue

import unittest
import apache_beam as beam


def ConvertToTimestampedValue(plant: Dict[str, Any]) -> TimestampedValue[str]:
  return TimestampedValue[str](plant["name"], plant["season"])


def ConvertToTimestampedValue_1(plant: Dict[str, Any]) -> TimestampedValue:
  return TimestampedValue(plant["name"], plant["season"])


def ConvertToTimestampedValue_2(
    plant: Dict[str, Any]) -> TimestampedValue[List[str]]:
  return TimestampedValue[List[str]](plant["name"], plant["season"])


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

  def test_pcoll_hints(self):
    for fn in (ConvertToTimestampedValue, ConvertToTimestampedValue_1):
      pc = beam.Map(fn)
      ht = pc.default_type_hints()
      assert len(ht) == 3
      # assert ht.output_types[0][0] == TimestampedValue[str]
      assert ht.output_types[0][0] == Any

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
    with self.assertRaises(TypeCheckError):
      with beam.Pipeline(options=self.opts) as p:
        _ = (
            p
            | "Garden plants" >> beam.Create(self.data_1)
            | "With timestamps" >> beam.Map(ConvertToTimestampedValue)
            | beam.Map(print))

  def test_opts_with_check_wrong_data_list_str(self):
    with self.assertRaises(TypeCheckError):
      with beam.Pipeline(options=self.opts) as p:
        _ = (
            p
            | "Garden plants" >> beam.Create(self.data_1)
            | "With timestamps" >> beam.Map(ConvertToTimestampedValue_2)
            | beam.Map(print))

    with self.assertRaises(TypeCheckError):
      with beam.Pipeline(options=self.opts) as p:
        _ = (
            p
            | "Garden plants" >> beam.Create(self.data_3)
            | "With timestamps" >> beam.Map(ConvertToTimestampedValue_2)
            | beam.Map(print))


if __name__ == '__main__':
  unittest.main()
