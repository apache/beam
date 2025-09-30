import apache_beam as beam
from apache_beam import metrics
from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner

import unittest


class ProgrammingGuideTest(unittest.TestCase):
  def test_metrics_example(self):
    class MyMetricsDoFn(beam.DoFn):
      def __init__(self):
        super().__init__()
        self.counter = metrics.Metrics.counter("namespace", "counter1")

      def process(self, element):
        self.counter.inc()
        yield element

    with beam.Pipeline(runner=BundleBasedDirectRunner()) as p:
      _ = p | beam.Create([1, 2, 3]) | beam.ParDo(MyMetricsDoFn())

    metrics_ = p.result.metrics().query(
        metrics.MetricsFilter().with_namespace("namespace").with_name(
            "counter1"))

    for metric in metrics_["counters"]:
      print(metric)

    # Not in example but just to confirm that anything is returned
    assert metrics_["counters"]
