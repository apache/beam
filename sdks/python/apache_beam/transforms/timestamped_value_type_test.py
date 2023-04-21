from apache_beam.transforms.core import DoFn
from apache_beam.typehints import Iterable
from apache_beam.transforms.window import TimestampedValue

import unittest
import apache_beam as beam


class MyDoFn(DoFn):
  def process(self, plant, timestamp=beam.DoFn.TimestampParam) -> \
      Iterable[TimestampedValue[dict]]:
    pass


class MyDoFn_1(DoFn):
  def process(self, plant, timestamp=beam.DoFn.TimestampParam) -> \
      Iterable[TimestampedValue]:
    pass


class TypeCheckTimestampedValueTestCase(unittest.TestCase):
  def setUp(self):
    self.opts = beam.options.pipeline_options.PipelineOptions(
        performance_runtime_type_check=True)
    self.opts_no_check = beam.options.pipeline_options.PipelineOptions(
        performance_runtime_type_check=False)
    self.data = [
        {
            "name": "Strawberry", "season": 1585699200
        },  # April, 2020
    ]

  def test_timestamped_value(self):

    th = MyDoFn().get_type_hints()
    assert th

  def test_opts_with_check(self):
    with beam.Pipeline(options=self.opts) as p:
      _ = (
          p
          | "Garden plants" >> beam.Create(self.data)
          | "With timestamps" >> beam.Map(
              lambda plant: TimestampedValue[dict](plant, plant["season"]))
          | "Test timestamps" >> beam.ParDo(MyDoFn())
          | beam.Map(print))

  def test_opts_with_check_no_type(self):
    with beam.Pipeline(options=self.opts) as p:
      _ = (
          p
          | "Garden plants" >> beam.Create(self.data)
          | "With timestamps" >>
          beam.Map(lambda plant: TimestampedValue(plant, plant["season"]))
          | "Test timestamps" >> beam.ParDo(MyDoFn_1())
          | beam.Map(print))


if __name__ == '__main__':
  unittest.main()
