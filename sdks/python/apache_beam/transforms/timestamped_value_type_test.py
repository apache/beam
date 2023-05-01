from apache_beam.typehints import Dict, Any
from apache_beam.transforms.window import TimestampedValue

import unittest
import apache_beam as beam


def ConvertToTimestampedValue(plant: Dict[str, Any]) -> TimestampedValue[str]:
  return TimestampedValue[str](plant["name"], plant["season"])


def ConvertToTimestampedValue_1(plant: Dict[str, Any]) -> TimestampedValue:
  return TimestampedValue(plant["name"], plant["season"])


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

  def test_opts_with_check_wrong_data(self):
    with self.assertRaises(ValueError):
      with beam.Pipeline(options=self.opts) as p:
        _ = (
            p
            | "Garden plants" >> beam.Create(self.data_1)
            | "With timestamps" >> beam.Map(ConvertToTimestampedValue)
            | beam.Map(print))


if __name__ == '__main__':
  unittest.main()
