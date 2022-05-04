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

"""Tests for apache_beam.ml.base."""

import pickle
import unittest
from typing import Any
from typing import Iterable

import apache_beam as beam
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.ml.inference import base
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class FakeModel:
  def predict(self, example: int) -> int:
    return example + 1


class FakeInferenceRunner(base.InferenceRunner):
  def __init__(self, clock=None):
    self._mock_clock = clock

  def run_inference(self, batch: Any, model: Any) -> Iterable[Any]:
    if self._mock_clock:
      self._mock_clock.current_time += 3000
    for example in batch:
      yield model.predict(example)


class FakeModelLoader(base.ModelLoader):
  def __init__(self, clock=None):
    self._mock_clock = clock

  def load_model(self):
    if self._mock_clock:
      self._mock_clock.current_time += 50000
    return FakeModel()

  def get_inference_runner(self):
    return FakeInferenceRunner(self._mock_clock)


class MockClock(base._Clock):
  def __init__(self):
    self.current_time = 10000

  def get_current_time_in_microseconds(self) -> int:
    return self.current_time


class ExtractInferences(beam.DoFn):
  def process(self, prediction_result):
    yield prediction_result.inference


class RunInferenceBaseTest(unittest.TestCase):
  def test_run_inference_impl_simple_examples(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      expected = [example + 1 for example in examples]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(FakeModelLoader())
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_impl_with_keyed_examples(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      keyed_examples = [(i, example) for i, example in enumerate(examples)]
      expected = [(i, example + 1) for i, example in enumerate(examples)]
      pcoll = pipeline | 'start' >> beam.Create(keyed_examples)
      actual = pcoll | base.RunInference(FakeModelLoader())
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_counted_metrics(self):
    pipeline = TestPipeline()
    examples = [1, 5, 3, 10]
    pcoll = pipeline | 'start' >> beam.Create(examples)
    _ = pcoll | base.RunInference(FakeModelLoader())
    run_result = pipeline.run()
    run_result.wait_until_finish()

    metric_results = (
        run_result.metrics().query(MetricsFilter().with_name('num_inferences')))
    num_inferences_counter = metric_results['counters'][0]
    self.assertEqual(num_inferences_counter.committed, 4)

    inference_request_batch_size = run_result.metrics().query(
        MetricsFilter().with_name('inference_request_batch_size'))
    self.assertTrue(inference_request_batch_size['distributions'])
    self.assertEqual(
        inference_request_batch_size['distributions'][0].result.sum, 4)
    inference_request_batch_byte_size = run_result.metrics().query(
        MetricsFilter().with_name('inference_request_batch_byte_size'))
    self.assertTrue(inference_request_batch_byte_size['distributions'])
    self.assertGreaterEqual(
        inference_request_batch_byte_size['distributions'][0].result.sum,
        len(pickle.dumps(examples)))
    inference_request_batch_byte_size = run_result.metrics().query(
        MetricsFilter().with_name('model_byte_size'))
    self.assertTrue(inference_request_batch_byte_size['distributions'])

  def test_timing_metrics(self):
    pipeline = TestPipeline()
    examples = [1, 5, 3, 10]
    pcoll = pipeline | 'start' >> beam.Create(examples)
    mock_clock = MockClock()
    _ = pcoll | base.RunInference(
        FakeModelLoader(clock=mock_clock), clock=mock_clock)
    res = pipeline.run()
    res.wait_until_finish()

    metric_results = (
        res.metrics().query(
            MetricsFilter().with_name('inference_batch_latency_micro_secs')))
    batch_latency = metric_results['distributions'][0]
    self.assertEqual(batch_latency.result.count, 3)
    self.assertEqual(batch_latency.result.mean, 3000)

    metric_results = (
        res.metrics().query(
            MetricsFilter().with_name('load_model_latency_milli_secs')))
    load_model_latency = metric_results['distributions'][0]
    self.assertEqual(load_model_latency.result.count, 1)
    self.assertEqual(load_model_latency.result.mean, 50)


if __name__ == '__main__':
  unittest.main()
