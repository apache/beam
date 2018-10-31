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
"""
To run test on DirectRunner

python setup.py nosetests \
    --test-pipeline-options="
    --number_of_counter_operations=1000
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\":15,
    \"bundle_size_distribution_type\": \"const\",
    \"bundle_size_distribution_param\": 1,
    \"force_initial_num_bundles\": 0
    }'" \
    --tests apache_beam.testing.load_tests.par_do_test

To run test on other runner (ex. Dataflow):

python setup.py nosetests \
    --test-pipeline-options="
        --runner=TestDataflowRunner
        --project=...
        --staging_location=gs://...
        --temp_location=gs://...
        --sdk_location=./dist/apache-beam-x.x.x.dev0.tar.gz
        --output=gc
        --number_of_counter_operations=1000
        --input_options='{
        \"num_records\": 1000,
        \"key_size\": 5,
        \"value_size\":15,
        \"bundle_size_distribution_type\": \"const\",
        \"bundle_size_distribution_param\": 1,
        \"force_initial_num_bundles\": 0
        }'" \
    --tests apache_beam.testing.load_tests.par_do_test

"""

from __future__ import absolute_import

import json
import logging
import time
import unittest

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.test_pipeline import TestPipeline


class ParDoTest(unittest.TestCase):
  def parseTestPipelineOptions(self):
    return {'numRecords': self.inputOptions.get('num_records'),
            'keySizeBytes': self.inputOptions.get('key_size'),
            'valueSizeBytes': self.inputOptions.get('value_size'),
            'bundleSizeDistribution': {
                'type': self.inputOptions.get(
                    'bundle_size_distribution_type', 'const'
                ),
                'param': self.inputOptions.get(
                    'bundle_size_distribution_param', 0
                )
            },
            'forceNumInitialBundles': self.inputOptions.get(
                'force_initial_num_bundles', 0
            )
           }

  def setUp(self):
    self.pipeline = TestPipeline(is_integration_test=True)
    self.output = self.pipeline.get_option('output')
    self.iterations = self.pipeline.get_option('number_of_counter_operations')
    self.inputOptions = json.loads(self.pipeline.get_option('input_options'))

  class _MeasureTime(beam.DoFn):
    def __init__(self):
      self.runtime_start = Metrics.distribution('pardo', 'runtime.start')
      self.runtime_end = Metrics.distribution('pardo', 'runtime.end')

    def start_bundle(self):
      self.runtime_start.update(time.time())

    def finish_bundle(self):
      self.runtime_end.update(time.time())

    def process(self, element):
      yield element

  class _GetElement(beam.DoFn):
    def __init__(self):
      self.counter = Metrics.counter('pardo', 'total_bytes.count')

    def process(self, element):
      _, value = element
      for i in range(len(value)):
        self.counter.inc(i)
      yield element

  def testParDo(self):
    if self.iterations is None:
      num_runs = 1
    else:
      num_runs = int(self.iterations)

    with self.pipeline as p:
      pc = (p
            | 'Read synthetic' >> beam.io.Read(
                synthetic_pipeline.SyntheticSource(
                    self.parseTestPipelineOptions()
                ))
            | 'Measure time' >> beam.ParDo(MeasureTime())
           )

      for i in range(num_runs):
        label = 'Step: %d' % i
        pc = (pc
              | label >> beam.ParDo(self._GetElement()))

      if self.output is not None:
        # pylint: disable=expression-not-assigned
        (pc
         | "Write" >> beam.io.WriteToText(self.output)
        )

      result = p.run()
      result.wait_until_finish()
      metrics = result.metrics().query()
      for counter in metrics['counters']:
        logging.info("Counter: %s", counter)

      for dist in metrics['distributions']:
        logging.info("Distribution: %s", dist)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
