import unittest

import apache_beam as beam
from apache_beam import metrics
from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner


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
      _ = (p | beam.Create([1, 2, 3]) | beam.ParDo(MyMetricsDoFn()))

    metrics_filter = metrics.MetricsFilter().with_name("counter1")
    query_result = p.result.metrics().query(metrics_filter)

    for metric in query_result["counters"]:
      print(metric)

    # Not in example but just to confirm that anything is returned
    assert query_result["counters"]


if __name__ == '__main__':
  unittest.main()
