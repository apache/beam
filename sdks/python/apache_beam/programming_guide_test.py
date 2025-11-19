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
