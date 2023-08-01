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
import math
import os
import pickle
import time
import unittest
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Mapping
from typing import Optional
from typing import Sequence

import pytest

import apache_beam as beam
from apache_beam.examples.inference import run_inference_side_inputs
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.ml.inference import base
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.periodicsequence import TimestampedValue
from apache_beam.utils import multi_process_shared


class FakeModel:
  def predict(self, example: int) -> int:
    return example + 1


class FakeStatefulModel:
  def __init__(self, state: int):
    if state == 100:
      raise Exception('Oh no')
    self._state = state

  def predict(self, example: int) -> int:
    return self._state

  def increment_state(self, amount: int):
    self._state += amount


class FakeModelHandler(base.ModelHandler[int, int, FakeModel]):
  def __init__(
      self,
      clock=None,
      min_batch_size=1,
      max_batch_size=9999,
      multi_process_shared=False,
      state=None,
      **kwargs):
    self._fake_clock = clock
    self._min_batch_size = min_batch_size
    self._max_batch_size = max_batch_size
    self._env_vars = kwargs.get('env_vars', {})
    self._multi_process_shared = multi_process_shared
    self._state = state

  def load_model(self):
    if self._fake_clock:
      self._fake_clock.current_time_ns += 500_000_000  # 500ms
    if self._state is not None:
      return FakeStatefulModel(self._state)
    return FakeModel()

  def run_inference(
      self,
      batch: Sequence[int],
      model: FakeModel,
      inference_args=None) -> Iterable[int]:
    multi_process_shared_loaded = "multi_process_shared" in str(type(model))
    if self._multi_process_shared != multi_process_shared_loaded:
      raise Exception(
          f'Loaded model of type {type(model)}, was' +
          f'{"" if self._multi_process_shared else " not"} ' +
          'expecting multi_process_shared_model')
    if self._fake_clock:
      self._fake_clock.current_time_ns += 3_000_000  # 3 milliseconds
    for example in batch:
      yield model.predict(example)

  def update_model_path(self, model_path: Optional[str] = None):
    pass

  def batch_elements_kwargs(self):
    return {
        'min_batch_size': self._min_batch_size,
        'max_batch_size': self._max_batch_size
    }

  def share_model_across_processes(self):
    return self._multi_process_shared


class FakeModelHandlerReturnsPredictionResult(
    base.ModelHandler[int, base.PredictionResult, FakeModel]):
  def __init__(
      self,
      clock=None,
      model_id='fake_model_id_default',
      multi_process_shared=False):
    self.model_id = model_id
    self._fake_clock = clock
    self._env_vars = {}
    self._multi_process_shared = multi_process_shared

  def load_model(self):
    return FakeModel()

  def run_inference(
      self,
      batch: Sequence[int],
      model: FakeModel,
      inference_args=None) -> Iterable[base.PredictionResult]:
    multi_process_shared_loaded = "multi_process_shared" in str(type(model))
    if self._multi_process_shared != multi_process_shared_loaded:
      raise Exception(
          f'Loaded model of type {type(model)}, was' +
          f'{"" if self._multi_process_shared else " not"} ' +
          'expecting multi_process_shared_model')
    for example in batch:
      yield base.PredictionResult(
          model_id=self.model_id,
          example=example,
          inference=model.predict(example))

  def update_model_path(self, model_path: Optional[str] = None):
    self.model_id = model_path if model_path else self.model_id

  def share_model_across_processes(self):
    return self._multi_process_shared


class FakeModelHandlerNoEnvVars(base.ModelHandler[int, int, FakeModel]):
  def __init__(
      self, clock=None, min_batch_size=1, max_batch_size=9999, **kwargs):
    self._fake_clock = clock
    self._min_batch_size = min_batch_size
    self._max_batch_size = max_batch_size

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

  def update_model_path(self, model_path: Optional[str] = None):
    pass

  def batch_elements_kwargs(self):
    return {
        'min_batch_size': self._min_batch_size,
        'max_batch_size': self._max_batch_size
    }


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


class FakeModelHandlerFailsOnInferenceArgs(FakeModelHandler):
  def run_inference(self, batch, unused_model, inference_args=None):
    raise ValueError(
        'run_inference should not be called because error should already be '
        'thrown from the validate_inference_args check.')


class FakeModelHandlerExpectedInferenceArgs(FakeModelHandler):
  def run_inference(self, batch, unused_model, inference_args=None):
    if not inference_args:
      raise ValueError('inference_args should exist')
    return batch

  def validate_inference_args(self, inference_args):
    pass


class RunInferenceBaseTest(unittest.TestCase):
  def test_run_inference_impl_simple_examples(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      expected = [example + 1 for example in examples]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(FakeModelHandler())
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_impl_simple_examples_multi_process_shared(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      expected = [example + 1 for example in examples]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(
          FakeModelHandler(multi_process_shared=True))
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

  def test_run_inference_impl_with_keyed_examples_multi_process_shared(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      keyed_examples = [(i, example) for i, example in enumerate(examples)]
      expected = [(i, example + 1) for i, example in enumerate(examples)]
      pcoll = pipeline | 'start' >> beam.Create(keyed_examples)
      actual = pcoll | base.RunInference(
          base.KeyedModelHandler(FakeModelHandler(multi_process_shared=True)))
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_impl_with_maybe_keyed_examples_multi_process_shared(
      self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      keyed_examples = [(i, example) for i, example in enumerate(examples)]
      expected = [example + 1 for example in examples]
      keyed_expected = [(i, example + 1) for i, example in enumerate(examples)]
      model_handler = base.MaybeKeyedModelHandler(
          FakeModelHandler(multi_process_shared=True))

      pcoll = pipeline | 'Unkeyed' >> beam.Create(examples)
      actual = pcoll | 'RunUnkeyed' >> base.RunInference(model_handler)
      assert_that(actual, equal_to(expected), label='CheckUnkeyed')

      keyed_pcoll = pipeline | 'Keyed' >> beam.Create(keyed_examples)
      keyed_actual = keyed_pcoll | 'RunKeyed' >> base.RunInference(
          model_handler)
      assert_that(keyed_actual, equal_to(keyed_expected), label='CheckKeyed')

  def test_run_inference_preprocessing(self):
    def mult_two(example: str) -> int:
      return int(example) * 2

    with TestPipeline() as pipeline:
      examples = ["1", "5", "3", "10"]
      expected = [int(example) * 2 + 1 for example in examples]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(
          FakeModelHandler().with_preprocess_fn(mult_two))
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_preprocessing_multiple_fns(self):
    def add_one(example: str) -> int:
      return int(example) + 1

    def mult_two(example: int) -> int:
      return example * 2

    with TestPipeline() as pipeline:
      examples = ["1", "5", "3", "10"]
      expected = [(int(example) + 1) * 2 + 1 for example in examples]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(
          FakeModelHandler().with_preprocess_fn(mult_two).with_preprocess_fn(
              add_one))
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_postprocessing(self):
    def mult_two(example: int) -> str:
      return str(example * 2)

    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      expected = [str((example + 1) * 2) for example in examples]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(
          FakeModelHandler().with_postprocess_fn(mult_two))
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_postprocessing_multiple_fns(self):
    def add_one(example: int) -> str:
      return str(int(example) + 1)

    def mult_two(example: int) -> int:
      return example * 2

    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      expected = [str(((example + 1) * 2) + 1) for example in examples]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(
          FakeModelHandler().with_postprocess_fn(mult_two).with_postprocess_fn(
              add_one))
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_preprocessing_dlq(self):
    def mult_two(example: str) -> int:
      if example == "5":
        raise Exception("TEST")
      return int(example) * 2

    with TestPipeline() as pipeline:
      examples = ["1", "5", "3", "10"]
      expected = [3, 7, 21]
      expected_bad = ["5"]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      main, other = pcoll | base.RunInference(
          FakeModelHandler().with_preprocess_fn(mult_two)
          ).with_exception_handling()
      assert_that(main, equal_to(expected), label='assert:inferences')
      assert_that(
          other.failed_inferences, equal_to([]), label='assert:bad_infer')

      # bad will be in form [element, error]. Just pull out bad element.
      bad_without_error = other.failed_preprocessing[0] | beam.Map(
          lambda x: x[0])
      assert_that(
          bad_without_error, equal_to(expected_bad), label='assert:failures')

  def test_run_inference_postprocessing_dlq(self):
    def mult_two(example: int) -> str:
      if example == 6:
        raise Exception("TEST")
      return str(example * 2)

    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      expected = ["4", "8", "22"]
      expected_bad = [6]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      main, other = pcoll | base.RunInference(
          FakeModelHandler().with_postprocess_fn(mult_two)
          ).with_exception_handling()
      assert_that(main, equal_to(expected), label='assert:inferences')
      assert_that(
          other.failed_inferences, equal_to([]), label='assert:bad_infer')

      # bad will be in form [element, error]. Just pull out bad element.
      bad_without_error = other.failed_postprocessing[0] | beam.Map(
          lambda x: x[0])
      assert_that(
          bad_without_error, equal_to(expected_bad), label='assert:failures')

  def test_run_inference_pre_and_post_processing_dlq(self):
    def mult_two_pre(example: str) -> int:
      if example == "5":
        raise Exception("TEST")
      return int(example) * 2

    def mult_two_post(example: int) -> str:
      if example == 7:
        raise Exception("TEST")
      return str(example * 2)

    with TestPipeline() as pipeline:
      examples = ["1", "5", "3", "10"]
      expected = ["6", "42"]
      expected_bad_pre = ["5"]
      expected_bad_post = [7]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      main, other = pcoll | base.RunInference(
          FakeModelHandler().with_preprocess_fn(
            mult_two_pre
            ).with_postprocess_fn(
              mult_two_post
              )).with_exception_handling()
      assert_that(main, equal_to(expected), label='assert:inferences')
      assert_that(
          other.failed_inferences, equal_to([]), label='assert:bad_infer')

      # bad will be in form [elements, error]. Just pull out bad element.
      bad_without_error_pre = other.failed_preprocessing[0] | beam.Map(
          lambda x: x[0])
      assert_that(
          bad_without_error_pre,
          equal_to(expected_bad_pre),
          label='assert:failures_pre')

      # bad will be in form [elements, error]. Just pull out bad element.
      bad_without_error_post = other.failed_postprocessing[0] | beam.Map(
          lambda x: x[0])
      assert_that(
          bad_without_error_post,
          equal_to(expected_bad_post),
          label='assert:failures_post')

  def test_run_inference_keyed_pre_and_post_processing(self):
    def mult_two(element):
      return (element[0], element[1] * 2)

    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      keyed_examples = [(i, example) for i, example in enumerate(examples)]
      expected = [
          (i, ((example * 2) + 1) * 2) for i, example in enumerate(examples)
      ]
      pcoll = pipeline | 'start' >> beam.Create(keyed_examples)
      actual = pcoll | base.RunInference(
          base.KeyedModelHandler(FakeModelHandler()).with_preprocess_fn(
              mult_two).with_postprocess_fn(mult_two))
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_maybe_keyed_pre_and_post_processing(self):
    def mult_two(element):
      return element * 2

    def mult_two_keyed(element):
      return (element[0], element[1] * 2)

    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      keyed_examples = [(i, example) for i, example in enumerate(examples)]
      expected = [((2 * example) + 1) * 2 for example in examples]
      keyed_expected = [
          (i, ((2 * example) + 1) * 2) for i, example in enumerate(examples)
      ]
      model_handler = base.MaybeKeyedModelHandler(FakeModelHandler())

      pcoll = pipeline | 'Unkeyed' >> beam.Create(examples)
      actual = pcoll | 'RunUnkeyed' >> base.RunInference(
          model_handler.with_preprocess_fn(mult_two).with_postprocess_fn(
              mult_two))
      assert_that(actual, equal_to(expected), label='CheckUnkeyed')

      keyed_pcoll = pipeline | 'Keyed' >> beam.Create(keyed_examples)
      keyed_actual = keyed_pcoll | 'RunKeyed' >> base.RunInference(
          model_handler.with_preprocess_fn(mult_two_keyed).with_postprocess_fn(
              mult_two_keyed))
      assert_that(keyed_actual, equal_to(keyed_expected), label='CheckKeyed')

  def test_run_inference_impl_dlq(self):
    with TestPipeline() as pipeline:
      examples = [1, 'TEST', 3, 10, 'TEST2']
      expected_good = [2, 4, 11]
      expected_bad = ['TEST', 'TEST2']
      pcoll = pipeline | 'start' >> beam.Create(examples)
      main, other = pcoll | base.RunInference(
          FakeModelHandler(
            min_batch_size=1,
            max_batch_size=1
          )).with_exception_handling()
      assert_that(main, equal_to(expected_good), label='assert:inferences')

      # bad.failed_inferences will be in form [batch[elements], error].
      # Just pull out bad element.
      bad_without_error = other.failed_inferences | beam.Map(lambda x: x[0][0])
      assert_that(
          bad_without_error, equal_to(expected_bad), label='assert:failures')

  def test_run_inference_impl_inference_args(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      inference_args = {'key': True}
      actual = pcoll | base.RunInference(
          FakeModelHandlerExpectedInferenceArgs(),
          inference_args=inference_args)
      assert_that(actual, equal_to(examples), label='assert:inferences')

  def test_run_inference_metrics_with_custom_namespace(self):
    metrics_namespace = 'my_custom_namespace'
    pipeline = TestPipeline()
    examples = [1, 5, 3, 10]
    pcoll = pipeline | 'start' >> beam.Create(examples)
    _ = pcoll | base.RunInference(
        FakeModelHandler(), metrics_namespace=metrics_namespace)
    result = pipeline.run()
    result.wait_until_finish()

    metrics_filter = MetricsFilter().with_namespace(namespace=metrics_namespace)
    metrics = result.metrics().query(metrics_filter)
    assert len(metrics['counters']) != 0
    assert len(metrics['distributions']) != 0

    metrics_filter = MetricsFilter().with_namespace(namespace='fake_namespace')
    metrics = result.metrics().query(metrics_filter)
    assert len(metrics['counters']) == len(metrics['distributions']) == 0

  def test_unexpected_inference_args_passed(self):
    with self.assertRaisesRegex(ValueError, r'inference_args were provided'):
      with TestPipeline() as pipeline:
        examples = [1, 5, 3, 10]
        pcoll = pipeline | 'start' >> beam.Create(examples)
        inference_args = {'key': True}
        _ = pcoll | base.RunInference(
            FakeModelHandlerFailsOnInferenceArgs(),
            inference_args=inference_args)

  def test_increment_failed_batches_counter(self):
    with self.assertRaises(ValueError):
      with TestPipeline() as pipeline:
        examples = [7]
        pcoll = pipeline | 'start' >> beam.Create(examples)
        _ = pcoll | base.RunInference(FakeModelHandlerExpectedInferenceArgs())
        run_result = pipeline.run()
        run_result.wait_until_finish()

        metric_results = (
            run_result.metrics().query(
                MetricsFilter().with_name('failed_batches_counter')))
        num_failed_batches_counter = metric_results['counters'][0]
        self.assertEqual(num_failed_batches_counter.committed, 3)
        # !!!: The above will need to be updated if retry behavior changes

  def test_failed_batches_counter_no_failures(self):
    pipeline = TestPipeline()
    examples = [7]
    pcoll = pipeline | 'start' >> beam.Create(examples)
    inference_args = {'key': True}
    _ = pcoll | base.RunInference(
        FakeModelHandlerExpectedInferenceArgs(), inference_args=inference_args)
    run_result = pipeline.run()
    run_result.wait_until_finish()

    metric_results = (
        run_result.metrics().query(
            MetricsFilter().with_name('failed_batches_counter')))
    self.assertEqual(len(metric_results['counters']), 0)

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

  def test_model_handler_compatibility(self):
    # ** IMPORTANT ** Do not change this test to make your PR pass without
    # first reading below.
    # Be certain that the modification will not break third party
    # implementations of ModelHandler.
    # See issue https://github.com/apache/beam/issues/23484
    # If this test fails, likely third party implementations of
    # ModelHandler will break.
    class ThirdPartyHandler(base.ModelHandler[int, int, FakeModel]):
      def __init__(self, custom_parameter=None):
        pass

      def load_model(self) -> FakeModel:
        return FakeModel()

      def run_inference(
          self,
          batch: Sequence[int],
          model: FakeModel,
          inference_args: Optional[Dict[str, Any]] = None) -> Iterable[int]:
        yield 0

      def get_num_bytes(self, batch: Sequence[int]) -> int:
        return 1

      def get_metrics_namespace(self) -> str:
        return 'ThirdParty'

      def get_resource_hints(self) -> dict:
        return {}

      def batch_elements_kwargs(self) -> Mapping[str, Any]:
        return {}

      def validate_inference_args(
          self, inference_args: Optional[Dict[str, Any]]):
        pass

    # This test passes if calling these methods does not cause
    # any runtime exceptions.
    third_party_model_handler = ThirdPartyHandler(custom_parameter=0)
    fake_model = third_party_model_handler.load_model()
    third_party_model_handler.run_inference([], fake_model)
    fake_inference_args = {'some_arg': 1}
    third_party_model_handler.run_inference([],
                                            fake_model,
                                            inference_args=fake_inference_args)
    third_party_model_handler.get_num_bytes([1, 2, 3])
    third_party_model_handler.get_metrics_namespace()
    third_party_model_handler.get_resource_hints()
    third_party_model_handler.batch_elements_kwargs()
    third_party_model_handler.validate_inference_args({})

  def test_run_inference_prediction_result_with_model_id(self):
    examples = [1, 5, 3, 10]
    expected = [
        base.PredictionResult(
            example=example,
            inference=example + 1,
            model_id='fake_model_id_default') for example in examples
    ]
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(
          FakeModelHandlerReturnsPredictionResult())
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_with_iterable_side_input(self):
    test_pipeline = TestPipeline()
    side_input = (
        test_pipeline | "CreateDummySideInput" >> beam.Create(
            [base.ModelMetadata(1, 1), base.ModelMetadata(2, 2)])
        | "ApplySideInputWindow" >> beam.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterProcessingTime(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING))

    test_pipeline.options.view_as(StandardOptions).streaming = True
    with self.assertRaises(ValueError) as e:
      _ = (
          test_pipeline
          | beam.Create([1, 2, 3, 4])
          | base.RunInference(
              FakeModelHandler(), model_metadata_pcoll=side_input))
      test_pipeline.run()

    self.assertTrue(
        'PCollection of size 2 with more than one element accessed as a '
        'singleton view. First two elements encountered are' in str(
            e.exception))

  def test_run_inference_with_iterable_side_input_multi_process_shared(self):
    test_pipeline = TestPipeline()
    side_input = (
        test_pipeline | "CreateDummySideInput" >> beam.Create(
            [base.ModelMetadata(1, 1), base.ModelMetadata(2, 2)])
        | "ApplySideInputWindow" >> beam.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterProcessingTime(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING))

    test_pipeline.options.view_as(StandardOptions).streaming = True
    with self.assertRaises(ValueError) as e:
      _ = (
          test_pipeline
          | beam.Create([1, 2, 3, 4])
          | base.RunInference(
              FakeModelHandler(multi_process_shared=True),
              model_metadata_pcoll=side_input))
      test_pipeline.run()

    self.assertTrue(
        'PCollection of size 2 with more than one element accessed as a '
        'singleton view. First two elements encountered are' in str(
            e.exception))

  def test_run_inference_empty_side_input(self):
    model_handler = FakeModelHandlerReturnsPredictionResult()
    main_input_elements = [1, 2]
    with TestPipeline(is_integration_test=False) as pipeline:
      side_pcoll = pipeline | "side" >> beam.Create([])
      result_pcoll = (
          pipeline
          | beam.Create(main_input_elements)
          | base.RunInference(model_handler, model_metadata_pcoll=side_pcoll))
      expected = [
          base.PredictionResult(ele, ele + 1, 'fake_model_id_default')
          for ele in main_input_elements
      ]

      assert_that(result_pcoll, equal_to(expected))

  def test_run_inference_side_input_in_batch(self):
    first_ts = math.floor(time.time()) - 30
    interval = 7

    sample_main_input_elements = ([
        first_ts - 2,
        first_ts + 1,
        first_ts + 8,
        first_ts + 15,
        first_ts + 22,
    ])

    sample_side_input_elements = [
        (first_ts + 1, base.ModelMetadata(model_id='', model_name='')),
        # if model_id is empty string, we use the default model
        # handler model URI.
        (
            first_ts + 8,
            base.ModelMetadata(
                model_id='fake_model_id_1', model_name='fake_model_id_1')),
        (
            first_ts + 15,
            base.ModelMetadata(
                model_id='fake_model_id_2', model_name='fake_model_id_2'))
    ]

    model_handler = FakeModelHandlerReturnsPredictionResult()

    # applying GroupByKey to utilize windowing according to
    # https://beam.apache.org/documentation/programming-guide/#windowing-bounded-collections
    class _EmitElement(beam.DoFn):
      def process(self, element):
        for e in element:
          yield e

    with TestPipeline() as pipeline:
      side_input = (
          pipeline
          |
          "CreateSideInputElements" >> beam.Create(sample_side_input_elements)
          | beam.Map(lambda x: TimestampedValue(x[1], x[0]))
          | beam.WindowInto(
              window.FixedWindows(interval),
              accumulation_mode=trigger.AccumulationMode.DISCARDING)
          | beam.Map(lambda x: ('key', x))
          | beam.GroupByKey()
          | beam.Map(lambda x: x[1])
          | "EmitSideInput" >> beam.ParDo(_EmitElement()))

      result_pcoll = (
          pipeline
          | beam.Create(sample_main_input_elements)
          | "MapTimeStamp" >> beam.Map(lambda x: TimestampedValue(x, x))
          | "ApplyWindow" >> beam.WindowInto(window.FixedWindows(interval))
          | beam.Map(lambda x: ('key', x))
          | "MainInputGBK" >> beam.GroupByKey()
          | beam.Map(lambda x: x[1])
          | beam.ParDo(_EmitElement())
          | "RunInference" >> base.RunInference(
              model_handler, model_metadata_pcoll=side_input))

      expected_model_id_order = [
          'fake_model_id_default',
          'fake_model_id_default',
          'fake_model_id_1',
          'fake_model_id_2',
          'fake_model_id_2'
      ]
      expected_result = [
          base.PredictionResult(
              example=sample_main_input_elements[i],
              inference=sample_main_input_elements[i] + 1,
              model_id=expected_model_id_order[i]) for i in range(5)
      ]

      assert_that(result_pcoll, equal_to(expected_result))

  def test_run_inference_side_input_in_batch_multi_process_shared(self):
    first_ts = math.floor(time.time()) - 30
    interval = 7

    sample_main_input_elements = ([
        first_ts - 2,
        first_ts + 1,
        first_ts + 8,
        first_ts + 15,
        first_ts + 22,
    ])

    sample_side_input_elements = [
        (first_ts + 1, base.ModelMetadata(model_id='', model_name='')),
        # if model_id is empty string, we use the default model
        # handler model URI.
        (
            first_ts + 8,
            base.ModelMetadata(
                model_id='fake_model_id_1', model_name='fake_model_id_1')),
        (
            first_ts + 15,
            base.ModelMetadata(
                model_id='fake_model_id_2', model_name='fake_model_id_2'))
    ]

    model_handler = FakeModelHandlerReturnsPredictionResult(
        multi_process_shared=True)

    # applying GroupByKey to utilize windowing according to
    # https://beam.apache.org/documentation/programming-guide/#windowing-bounded-collections
    class _EmitElement(beam.DoFn):
      def process(self, element):
        for e in element:
          yield e

    with TestPipeline() as pipeline:
      side_input = (
          pipeline
          |
          "CreateSideInputElements" >> beam.Create(sample_side_input_elements)
          | beam.Map(lambda x: TimestampedValue(x[1], x[0]))
          | beam.WindowInto(
              window.FixedWindows(interval),
              accumulation_mode=trigger.AccumulationMode.DISCARDING)
          | beam.Map(lambda x: ('key', x))
          | beam.GroupByKey()
          | beam.Map(lambda x: x[1])
          | "EmitSideInput" >> beam.ParDo(_EmitElement()))

      result_pcoll = (
          pipeline
          | beam.Create(sample_main_input_elements)
          | "MapTimeStamp" >> beam.Map(lambda x: TimestampedValue(x, x))
          | "ApplyWindow" >> beam.WindowInto(window.FixedWindows(interval))
          | beam.Map(lambda x: ('key', x))
          | "MainInputGBK" >> beam.GroupByKey()
          | beam.Map(lambda x: x[1])
          | beam.ParDo(_EmitElement())
          | "RunInference" >> base.RunInference(
              model_handler, model_metadata_pcoll=side_input))

      expected_model_id_order = [
          'fake_model_id_default',
          'fake_model_id_default',
          'fake_model_id_1',
          'fake_model_id_2',
          'fake_model_id_2'
      ]
      expected_result = [
          base.PredictionResult(
              example=sample_main_input_elements[i],
              inference=sample_main_input_elements[i] + 1,
              model_id=expected_model_id_order[i]) for i in range(5)
      ]

      assert_that(result_pcoll, equal_to(expected_result))

  @unittest.skipIf(
      not TestPipeline().get_pipeline_options().view_as(
          StandardOptions).streaming,
      "SideInputs to RunInference are only supported in streaming mode.")
  @pytest.mark.it_postcommit
  @pytest.mark.sickbay_direct
  @pytest.mark.it_validatesrunner
  def test_run_inference_with_side_inputin_streaming(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    test_pipeline.options.view_as(StandardOptions).streaming = True
    run_inference_side_inputs.run(
        test_pipeline.get_full_options_as_args(), save_main_session=False)

  def test_env_vars_set_correctly(self):
    handler_with_vars = FakeModelHandler(env_vars={'FOO': 'bar'})
    os.environ.pop('FOO', None)
    self.assertFalse('FOO' in os.environ)
    with TestPipeline() as pipeline:
      examples = [1, 2, 3]
      _ = (
          pipeline
          | 'start' >> beam.Create(examples)
          | base.RunInference(handler_with_vars))
      pipeline.run()
      self.assertTrue('FOO' in os.environ)
      self.assertTrue((os.environ['FOO']) == 'bar')

  def test_child_class_without_env_vars(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      expected = [example + 1 for example in examples]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(FakeModelHandlerNoEnvVars())
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_model_manager_loads_shared_model(self):
    mhs = {
        'key1': FakeModelHandler(state=1),
        'key2': FakeModelHandler(state=2),
        'key3': FakeModelHandler(state=3)
    }
    mm = base._ModelManager(mh_map=mhs)
    tag1 = mm.load('key1')
    # Use bad_mh's load function to make sure we're actually loading the
    # version already stored
    bad_mh = FakeModelHandler(state=100)
    model1 = multi_process_shared.MultiProcessShared(
        bad_mh.load_model, tag=tag1).acquire()
    self.assertEqual(1, model1.predict(10))

    tag2 = mm.load('key2')
    tag3 = mm.load('key3')
    model2 = multi_process_shared.MultiProcessShared(
        bad_mh.load_model, tag=tag2).acquire()
    model3 = multi_process_shared.MultiProcessShared(
        bad_mh.load_model, tag=tag3).acquire()
    self.assertEqual(2, model2.predict(10))
    self.assertEqual(3, model3.predict(10))

  def test_model_manager_evicts_models(self):
    mh1 = FakeModelHandler(state=1)
    mh2 = FakeModelHandler(state=2)
    mh3 = FakeModelHandler(state=3)
    mhs = {'key1': mh1, 'key2': mh2, 'key3': mh3}
    mm = base._ModelManager(mh_map=mhs, max_models=2)
    tag1 = mm.load('key1')
    sh1 = multi_process_shared.MultiProcessShared(mh1.load_model, tag=tag1)
    model1 = sh1.acquire()
    self.assertEqual(1, model1.predict(10))
    model1.increment_state(5)

    tag2 = mm.load('key2')
    tag3 = mm.load('key3')
    sh2 = multi_process_shared.MultiProcessShared(mh2.load_model, tag=tag2)
    model2 = sh2.acquire()
    sh3 = multi_process_shared.MultiProcessShared(mh3.load_model, tag=tag3)
    model3 = sh3.acquire()
    model2.increment_state(5)
    model3.increment_state(5)
    self.assertEqual(7, model2.predict(10))
    self.assertEqual(8, model3.predict(10))
    sh2.release(model2)
    sh3.release(model3)

    # model1 should have retained a valid reference to the model until released
    self.assertEqual(6, model1.predict(10))
    sh1.release(model1)

    # This should now have been garbage collected, so it now it should get
    # recreated and shouldn't have the state updates
    model1 = multi_process_shared.MultiProcessShared(
        mh1.load_model, tag=tag1).acquire()
    self.assertEqual(1, model1.predict(10))

    # These should not get recreated, so they should have the state updates
    model2 = multi_process_shared.MultiProcessShared(
        mh2.load_model, tag=tag2).acquire()
    self.assertEqual(7, model2.predict(10))
    model3 = multi_process_shared.MultiProcessShared(
        mh3.load_model, tag=tag3).acquire()
    self.assertEqual(8, model3.predict(10))

  def test_model_manager_evicts_correct_num_of_models_after_being_incremented(
      self):
    mh1 = FakeModelHandler(state=1)
    mh2 = FakeModelHandler(state=2)
    mh3 = FakeModelHandler(state=3)
    mhs = {'key1': mh1, 'key2': mh2, 'key3': mh3}
    mm = base._ModelManager(mh_map=mhs, max_models=1)
    mm.increment_max_models(1)
    tag1 = mm.load('key1')
    sh1 = multi_process_shared.MultiProcessShared(mh1.load_model, tag=tag1)
    model1 = sh1.acquire()
    self.assertEqual(1, model1.predict(10))
    model1.increment_state(5)
    self.assertEqual(6, model1.predict(10))
    sh1.release(model1)

    tag2 = mm.load('key2')
    tag3 = mm.load('key3')
    sh2 = multi_process_shared.MultiProcessShared(mh2.load_model, tag=tag2)
    model2 = sh2.acquire()
    sh3 = multi_process_shared.MultiProcessShared(mh3.load_model, tag=tag3)
    model3 = sh3.acquire()
    model2.increment_state(5)
    model3.increment_state(5)
    self.assertEqual(7, model2.predict(10))
    self.assertEqual(8, model3.predict(10))
    sh2.release(model2)
    sh3.release(model3)

    # This should get recreated, so it shouldn't have the state updates
    model1 = multi_process_shared.MultiProcessShared(
        mh1.load_model, tag=tag1).acquire()
    self.assertEqual(1, model1.predict(10))

    # These should not get recreated, so they should have the state updates
    model2 = multi_process_shared.MultiProcessShared(
        mh2.load_model, tag=tag2).acquire()
    self.assertEqual(7, model2.predict(10))
    model3 = multi_process_shared.MultiProcessShared(
        mh3.load_model, tag=tag3).acquire()
    self.assertEqual(8, model3.predict(10))

  def test_model_manager_fails_if_no_default_initially(self):
    mm = base._ModelManager(mh_map={})
    with self.assertRaisesRegex(ValueError, r'self._max_models is None'):
      mm.increment_max_models(5)


if __name__ == '__main__':
  unittest.main()
