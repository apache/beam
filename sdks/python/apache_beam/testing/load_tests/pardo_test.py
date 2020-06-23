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
This is ParDo load test with Synthetic Source. Besides of the standard
input options there are additional options:
* iterations - number of subsequent ParDo transforms to be performed,
* number_of_counters - number of counter metrics to be created for one ParDo
transform,
* number_of_counter_operations - number of operations on counters to be
performed in one ParDo,
* project (optional) - the gcp project in case of saving
metrics in Big Query (in case of Dataflow Runner
it is required to specify project of runner),
* publish_to_big_query - if metrics should be published in big query,
* metrics_dataset (optional) - name of BigQuery dataset where metrics
will be stored,
* metrics_table (optional) - name of BigQuery table where metrics
will be stored,
* input_options - options for Synthetic Sources.
* stateful - When true, this will use a stateful DoFn
* state_cache - When true, this will enable the Python state cache

Example test run:

python -m apache_beam.testing.load_tests.pardo_test \
    --test-pipeline-options="
    --iterations=1
    --number_of_counters=1
    --number_of_counter_operations=1
    --project=big-query-project
    --region=...
    --publish_to_big_query=true
    --metrics_dataset=python_load_tests
    --metrics_table=pardo
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\": 15
    }'"

or:

./gradlew -PloadTest.args="
    --publish_to_big_query=true
    --project=...
    --region=...
    --metrics_dataset=python_load_tests
    --metrics_table=pardo
    --input_options='{
      \"num_records\": 1,
      \"key_size\": 1,
      \"value_size\": 1}'
    --runner=DirectRunner" \
-PloadTest.mainClass=apache_beam.testing.load_tests.pardo_test \
-Prunner=DirectRunner :sdks:python:apache_beam:testing:load_tests:run
"""

# pytype: skip-file

from __future__ import absolute_import

import logging
import os
import time
from typing import Tuple

import apache_beam as beam
from apache_beam import pvalue
from apache_beam import typehints
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import AssignTimestamps
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureLatency
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.synthetic_pipeline import SyntheticSource
from apache_beam.transforms import userstate


class ParDoTest(LoadTest):
  def __init__(self):
    super(ParDoTest, self).__init__()
    self.iterations = self.get_option_or_default('iterations')
    self.number_of_counters = self.get_option_or_default(
        'number_of_counters', 1)
    self.number_of_operations = self.get_option_or_default(
        'number_of_counter_operations', 1)
    self.stateful = self.get_option_or_default('stateful', False)
    if self.get_option_or_default('state_cache', False):
      self.pipeline.options.view_as(DebugOptions).add_experiment(
          'state_cache_size=1000')

  def test(self):
    class CounterOperation(beam.DoFn):
      def __init__(self, number_of_counters, number_of_operations):
        self.number_of_operations = number_of_operations
        self.counters = []
        for i in range(number_of_counters):
          self.counters.append(
              Metrics.counter('do-not-publish', 'name-{}'.format(i)))

      state_param = beam.DoFn.StateParam(
          userstate.CombiningValueStateSpec(
              'count',
              beam.coders.IterableCoder(beam.coders.VarIntCoder()),
              sum)) if self.stateful else None

      def process(self, element, state=state_param):
        for _ in range(self.number_of_operations):
          for counter in self.counters:
            counter.inc()
          if state:
            state.add(1)
        yield element

    if self.get_option_or_default('streaming', False):
      source = (
          self.pipeline
          | 'LoadGenerator' >> StatefulLoadGenerator(self.input_options))
    else:
      source = (
          self.pipeline
          | 'Read synthetic' >> beam.io.Read(
              SyntheticSource(self.parse_synthetic_source_options())))

    pc = (
        source
        | 'Measure time: Start' >> beam.ParDo(
            MeasureTime(self.metrics_namespace))
        | 'Assign timestamps' >> beam.ParDo(AssignTimestamps()))

    for i in range(self.iterations):
      pc = (
          pc
          | 'Step: %d' % i >> beam.ParDo(
              CounterOperation(
                  self.number_of_counters, self.number_of_operations)))

    # pylint: disable=expression-not-assigned
    (
        pc
        |
        'Measure latency' >> beam.ParDo(MeasureLatency(self.metrics_namespace))
        |
        'Measure time: End' >> beam.ParDo(MeasureTime(self.metrics_namespace)))


class StatefulLoadGenerator(beam.PTransform):
  def __init__(self, input_options, num_keys=100):
    self.num_records = input_options['num_records']
    self.key_size = input_options['key_size']
    self.value_size = input_options['value_size']
    self.num_keys = num_keys

  @typehints.with_output_types(Tuple[bytes, bytes])
  class GenerateKeys(beam.DoFn):
    def __init__(self, num_keys, key_size):
      self.num_keys = num_keys
      self.key_size = key_size

    def process(self, impulse):
      for _ in range(self.num_keys):
        key = os.urandom(self.key_size)
        yield key, b''

  class GenerateLoad(beam.DoFn):
    state_spec = userstate.CombiningValueStateSpec(
        'bundles_remaining', combine_fn=sum)
    timer_spec = userstate.TimerSpec('timer', userstate.TimeDomain.WATERMARK)

    def __init__(self, num_records_per_key, value_size, bundle_size=1000):
      self.num_records_per_key = num_records_per_key
      self.payload = os.urandom(value_size)
      self.bundle_size = bundle_size
      self.time_fn = time.time

    def process(
        self,
        _element,
        records_remaining=beam.DoFn.StateParam(state_spec),
        timer=beam.DoFn.TimerParam(timer_spec)):
      records_remaining.add(self.num_records_per_key)
      timer.set(0)

    @userstate.on_timer(timer_spec)
    def process_timer(
        self,
        key=beam.DoFn.KeyParam,
        records_remaining=beam.DoFn.StateParam(state_spec),
        timer=beam.DoFn.TimerParam(timer_spec)):
      cur_bundle_size = min(self.bundle_size, records_remaining.read())
      for _ in range(cur_bundle_size):
        records_remaining.add(-1)
        yield key, self.payload
      if records_remaining.read() > 0:
        timer.set(0)

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin), (
        'Input to transform must be a PBegin but found %s' % pbegin)
    return (
        pbegin
        | 'Impulse' >> beam.Impulse()
        | 'GenerateKeys' >> beam.ParDo(
            StatefulLoadGenerator.GenerateKeys(self.num_keys, self.key_size))
        | 'GenerateLoad' >> beam.ParDo(
            StatefulLoadGenerator.GenerateLoad(
                self.num_records // self.num_keys, self.value_size)))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  ParDoTest().run()
