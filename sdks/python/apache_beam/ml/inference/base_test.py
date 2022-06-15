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
from typing import Iterable
from typing import Sequence

import apache_beam as beam
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.ml.inference import base
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class FakeModel:
  def predict(self, example: int) -> int:
    return example + 1


class FakeModelHandler(base.ModelHandler[int, int, FakeModel]):
  def __init__(self, clock=None):
    self._fake_clock = clock

  def load_model(self):
    if self._fake_clock:
      self._fake_clock.current_time_ns += 500_000_000  # 500ms
    return FakeModel()

  def run_inference(
      self,
      batch: Sequence[int],
      model: FakeModel,
      inference_args=None) -> Iterable[int]:
    if self._fake_clock:
      self._fake_clock.current_time_ns += 3_000_000  # 3 milliseconds
    for example in batch:
      yield model.predict(example)


class FakeClock:
  def __init__(self):
    # Start at 10 seconds.
    self.current_time_ns = 10_000_000_000

  def time_ns(self) -> int:
    return self.current_time_ns


class ExtractInferences(beam.DoFn):
  def process(self, prediction_result):
    yield prediction_result.inference


class FakeModelHandlerNeedsBigBatch(FakeModelHandler):
  def run_inference(self, batch, unused_model, inference_args=None):
    if len(batch) < 100:
      raise ValueError('Unexpectedly small batch')
    return batch

  def batch_elements_kwargs(self):
    return {'min_batch_size': 9999}


class FakeModelHandlerExtraInferenceArgs(FakeModelHandler):
  def run_inference(self, batch, unused_model, inference_args=None):
    if not inference_args:
      raise ValueError('inference_args should exist')
    return batch


class RunInferenceBaseTest(unittest.TestCase):
  def test_run_inference_impl_simple_examples(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      expected = [example + 1 for example in examples]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(FakeModelHandler())
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_impl_with_keyed_examples(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      keyed_examples = [(i, example) for i, example in enumerate(examples)]
      expected = [(i, example + 1) for i, example in enumerate(examples)]
      pcoll = pipeline | 'start' >> beam.Create(keyed_examples)
      actual = pcoll | base.RunInference(
          base.KeyedModelHandler(FakeModelHandler()))
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_impl_with_maybe_keyed_examples(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      keyed_examples = [(i, example) for i, example in enumerate(examples)]
      expected = [example + 1 for example in examples]
      keyed_expected = [(i, example + 1) for i, example in enumerate(examples)]
      model_handler = base.MaybeKeyedModelHandler(FakeModelHandler())

      pcoll = pipeline | 'Unkeyed' >> beam.Create(examples)
      actual = pcoll | 'RunUnkeyed' >> base.RunInference(model_handler)
      assert_that(actual, equal_to(expected), label='CheckUnkeyed')

      keyed_pcoll = pipeline | 'Keyed' >> beam.Create(keyed_examples)
      keyed_actual = keyed_pcoll | 'RunKeyed' >> base.RunInference(
          model_handler)
      assert_that(keyed_actual, equal_to(keyed_expected), label='CheckKeyed')

  def test_run_inference_impl_inference_args(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      inference_args = {'key': True}
      actual = pcoll | base.RunInference(
          FakeModelHandlerExtraInferenceArgs(), inference_args=inference_args)
      assert_that(actual, equal_to(examples), label='assert:inferences')

  def test_counted_metrics(self):
    pipeline = TestPipeline()
    examples = [1, 5, 3, 10]
    pcoll = pipeline | 'start' >> beam.Create(examples)
    _ = pcoll | base.RunInference(FakeModelHandler())
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
    fake_clock = FakeClock()
    _ = pcoll | base.RunInference(
        FakeModelHandler(clock=fake_clock), clock=fake_clock)
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
    self.assertEqual(load_model_latency.result.mean, 500)

  def test_forwards_batch_args(self):
    examples = list(range(100))
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(FakeModelHandlerNeedsBigBatch())
      assert_that(actual, equal_to(examples), label='assert:inferences')

  def test_run_inference_unkeyed_examples_with_keyed_model_handler(self):
    pipeline = TestPipeline()
    with self.assertRaises(TypeError):
      examples = [1, 3, 5]
      model_handler = base.KeyedModelHandler(FakeModelHandler())
      _ = (
          pipeline | 'Unkeyed' >> beam.Create(examples)
          | 'RunUnkeyed' >> base.RunInference(model_handler))
      pipeline.run()

  def test_run_inference_keyed_examples_with_unkeyed_model_handler(self):
    pipeline = TestPipeline()
    examples = [1, 3, 5]
    keyed_examples = [(i, example) for i, example in enumerate(examples)]
    model_handler = FakeModelHandler()
    with self.assertRaises(TypeError):
      _ = (
          pipeline | 'keyed' >> beam.Create(keyed_examples)
          | 'RunKeyed' >> base.RunInference(model_handler))
      pipeline.run()


if __name__ == '__main__':
  unittest.main()
