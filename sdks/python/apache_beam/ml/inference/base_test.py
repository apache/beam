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
import sys
import tempfile
import time
import unittest
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Union

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


class FakeSlowModel:
  def __init__(self, sleep_on_load_seconds=0, file_path_write_on_del=None):

    self._file_path_write_on_del = file_path_write_on_del
    time.sleep(sleep_on_load_seconds)

  def predict(self, example: int) -> int:
    time.sleep(example)
    return example

  def __del__(self):
    if self._file_path_write_on_del is not None:
      with open(self._file_path_write_on_del, 'a') as myfile:
        myfile.write('Deleted FakeSlowModel')


class FakeIncrementingModel:
  def __init__(self):
    self._state = 0

  def predict(self, example: int) -> int:
    self._state += 1
    return self._state


class FakeSlowModelHandler(base.ModelHandler[int, int, FakeModel]):
  def __init__(
      self,
      sleep_on_load: int,
      multi_process_shared=False,
      file_path_write_on_del=None):
    self._sleep_on_load = sleep_on_load
    self._multi_process_shared = multi_process_shared
    self._file_path_write_on_del = file_path_write_on_del

  def load_model(self):
    return FakeSlowModel(self._sleep_on_load, self._file_path_write_on_del)

  def run_inference(
      self,
      batch: Sequence[int],
      model: FakeModel,
      inference_args=None) -> Iterable[int]:
    for example in batch:
      yield model.predict(example)

  def share_model_across_processes(self) -> bool:
    return self._multi_process_shared

  def batch_elements_kwargs(self):
    return {'min_batch_size': 1, 'max_batch_size': 1}


class FakeModelHandler(base.ModelHandler[int, int, FakeModel]):
  def __init__(
      self,
      clock=None,
      min_batch_size=1,
      max_batch_size=9999,
      multi_process_shared=False,
      state=None,
      incrementing=False,
      max_copies=1,
      num_bytes_per_element=None,
      **kwargs):
    self._fake_clock = clock
    self._min_batch_size = min_batch_size
    self._max_batch_size = max_batch_size
    self._env_vars = kwargs.get('env_vars', {})
    self._multi_process_shared = multi_process_shared
    self._state = state
    self._incrementing = incrementing
    self._max_copies = max_copies
    self._num_bytes_per_element = num_bytes_per_element

  def load_model(self):
    assert (not self._incrementing or self._state is None)
    if self._fake_clock:
      self._fake_clock.current_time_ns += 500_000_000  # 500ms
    if self._incrementing:
      return FakeIncrementingModel()
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

  def model_copies(self):
    return self._max_copies

  def get_num_bytes(self, batch: Sequence[int]) -> int:
    if self._num_bytes_per_element:
      return self._num_bytes_per_element * len(batch)
    return super().get_num_bytes(batch)


class FakeModelHandlerReturnsPredictionResult(
    base.ModelHandler[int, base.PredictionResult, FakeModel]):
  def __init__(
      self,
      clock=None,
      model_id='fake_model_id_default',
      multi_process_shared=False,
      state=None):
    self.model_id = model_id
    self._fake_clock = clock
    self._env_vars = {}
    self._multi_process_shared = multi_process_shared
    self._state = state

  def load_model(self):
    if self._state is not None:
      return FakeStatefulModel(0)
    return FakeModel()

  def run_inference(
      self,
      batch: Sequence[int],
      model: Union[FakeModel, FakeStatefulModel],
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
      if self._state is not None:
        model.increment_state(1)  # type: ignore[union-attr]

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

  def test_run_inference_impl_simple_examples_multi_process_shared_multi_copy(
      self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      expected = [example + 1 for example in examples]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(
          FakeModelHandler(multi_process_shared=True, max_copies=4))
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_impl_multi_process_shared_incrementing_multi_copy(
      self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10, 1, 5, 3, 10, 1, 5, 3, 10, 1, 5, 3, 10]
      expected = [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(
          FakeModelHandler(
              multi_process_shared=True,
              max_copies=4,
              incrementing=True,
              max_batch_size=1))
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_impl_mps_nobatch_incrementing_multi_copy(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10, 1, 5, 3, 10, 1, 5, 3, 10, 1, 5, 3, 10]
      expected = [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4]
      batched_examples = [[example] for example in examples]
      pcoll = pipeline | 'start' >> beam.Create(batched_examples)
      actual = pcoll | base.RunInference(
          FakeModelHandler(
              multi_process_shared=True, max_copies=4,
              incrementing=True).with_no_batching())
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_impl_keyed_mps_incrementing_multi_copy(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10, 1, 5, 3, 10, 1, 5, 3, 10, 1, 5, 3, 10]
      keyed_examples = [('abc', example) for example in examples]
      expected = [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4]
      keyed_expected = [('abc', val) for val in expected]
      pcoll = pipeline | 'start' >> beam.Create(keyed_examples)
      actual = pcoll | base.RunInference(
          base.KeyedModelHandler(
              FakeModelHandler(
                  multi_process_shared=True,
                  max_copies=4,
                  incrementing=True,
                  max_batch_size=1)))
      assert_that(actual, equal_to(keyed_expected), label='assert:inferences')

  def test_run_inference_impl_with_keyed_examples(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      keyed_examples = [(i, example) for i, example in enumerate(examples)]
      expected = [(i, example + 1) for i, example in enumerate(examples)]
      pcoll = pipeline | 'start' >> beam.Create(keyed_examples)
      actual = pcoll | base.RunInference(
          base.KeyedModelHandler(FakeModelHandler()))
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_impl_with_keyed_examples_many_model_handlers(self):
    with TestPipeline() as pipeline:
      examples = [1, 5, 3, 10]
      keyed_examples = [(i, example) for i, example in enumerate(examples)]
      expected = [(i, example + 1) for i, example in enumerate(examples)]
      expected[0] = (0, 200)
      pcoll = pipeline | 'start' >> beam.Create(keyed_examples)
      mhs = [
          base.KeyModelMapping([0],
                               FakeModelHandler(
                                   state=200, multi_process_shared=True)),
          base.KeyModelMapping([1, 2, 3],
                               FakeModelHandler(multi_process_shared=True))
      ]
      actual = pcoll | base.RunInference(base.KeyedModelHandler(mhs))
      assert_that(actual, equal_to(expected), label='assert:inferences')

  def test_run_inference_impl_with_keyed_examples_many_model_handlers_metrics(
      self):
    pipeline = TestPipeline()
    examples = [1, 5, 3, 10]
    metrics_namespace = 'test_namespace'
    keyed_examples = [(i, example) for i, example in enumerate(examples)]
    pcoll = pipeline | 'start' >> beam.Create(keyed_examples)
    mhs = [
        base.KeyModelMapping([0],
                             FakeModelHandler(
                                 state=200, multi_process_shared=True)),
        base.KeyModelMapping([1, 2, 3],
                             FakeModelHandler(multi_process_shared=True))
    ]
    _ = pcoll | base.RunInference(
        base.KeyedModelHandler(mhs), metrics_namespace=metrics_namespace)
    result = pipeline.run()
    result.wait_until_finish()

    metrics_filter = MetricsFilter().with_namespace(namespace=metrics_namespace)
    metrics = result.metrics().query(metrics_filter)
    assert len(metrics['counters']) != 0
    assert len(metrics['distributions']) != 0

    metrics_filter = MetricsFilter().with_name('0-_num_inferences')
    metrics = result.metrics().query(metrics_filter)
    num_inferences_counter_key_0 = metrics['counters'][0]
    self.assertEqual(num_inferences_counter_key_0.committed, 1)

    metrics_filter = MetricsFilter().with_name('1-_num_inferences')
    metrics = result.metrics().query(metrics_filter)
    num_inferences_counter_key_1 = metrics['counters'][0]
    self.assertEqual(num_inferences_counter_key_1.committed, 3)

    metrics_filter = MetricsFilter().with_name('num_inferences')
    metrics = result.metrics().query(metrics_filter)
    num_inferences_counter_aggregate = metrics['counters'][0]
    self.assertEqual(num_inferences_counter_aggregate.committed, 4)

    metrics_filter = MetricsFilter().with_name('0-_failed_batches_counter')
    metrics = result.metrics().query(metrics_filter)
    failed_batches_counter_key_0 = metrics['counters']
    self.assertEqual(len(failed_batches_counter_key_0), 0)

    metrics_filter = MetricsFilter().with_name('failed_batches_counter')
    metrics = result.metrics().query(metrics_filter)
    failed_batches_counter_aggregate = metrics['counters']
    self.assertEqual(len(failed_batches_counter_aggregate), 0)

    metrics_filter = MetricsFilter().with_name(
        '0-_load_model_latency_milli_secs')
    metrics = result.metrics().query(metrics_filter)
    load_latency_dist_key_0 = metrics['distributions'][0]
    self.assertEqual(load_latency_dist_key_0.committed.count, 1)

    metrics_filter = MetricsFilter().with_name('load_model_latency_milli_secs')
    metrics = result.metrics().query(metrics_filter)
    load_latency_dist_aggregate = metrics['distributions'][0]
    self.assertEqual(load_latency_dist_aggregate.committed.count, 2)

  def test_run_inference_impl_with_keyed_examples_many_mhs_max_models_hint(
      self):
    pipeline = TestPipeline()
    examples = [1, 5, 3, 10, 2, 4, 6, 8, 9, 7, 1, 5, 3, 10, 2, 4, 6, 8, 9, 7]
    metrics_namespace = 'test_namespace'
    keyed_examples = [(i, example) for i, example in enumerate(examples)]
    pcoll = pipeline | 'start' >> beam.Create(keyed_examples)
    mhs = [
        base.KeyModelMapping([0, 2, 4, 6, 8],
                             FakeModelHandler(
                                 state=200, multi_process_shared=True)),
        base.KeyModelMapping(
            [1, 3, 5, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
            FakeModelHandler(multi_process_shared=True))
    ]
    _ = pcoll | base.RunInference(
        base.KeyedModelHandler(mhs, max_models_per_worker_hint=1),
        metrics_namespace=metrics_namespace)
    result = pipeline.run()
    result.wait_until_finish()

    metrics_filter = MetricsFilter().with_namespace(namespace=metrics_namespace)
    metrics = result.metrics().query(metrics_filter)
    assert len(metrics['counters']) != 0
    assert len(metrics['distributions']) != 0

    metrics_filter = MetricsFilter().with_name('load_model_latency_milli_secs')
    metrics = result.metrics().query(metrics_filter)
    load_latency_dist_aggregate = metrics['distributions'][0]
    # We should flip back and forth between models a bit since
    # max_models_per_worker_hint=1, but we shouldn't thrash forever
    # since most examples belong to the second ModelMapping
    self.assertGreater(load_latency_dist_aggregate.committed.count, 2)
    self.assertLess(load_latency_dist_aggregate.committed.count, 12)

  def test_keyed_many_model_handlers_validation(self):
    def mult_two(example: str) -> int:
      return int(example) * 2

    mhs = [
        base.KeyModelMapping(
            [0],
            FakeModelHandler(
                state=200,
                multi_process_shared=True).with_preprocess_fn(mult_two)),
        base.KeyModelMapping([1, 2, 3],
                             FakeModelHandler(multi_process_shared=True))
    ]
    with self.assertRaises(ValueError):
      base.KeyedModelHandler(mhs)

    mhs = [
        base.KeyModelMapping(
            [0],
            FakeModelHandler(
                state=200,
                multi_process_shared=True).with_postprocess_fn(mult_two)),
        base.KeyModelMapping([1, 2, 3],
                             FakeModelHandler(multi_process_shared=True))
    ]
    with self.assertRaises(ValueError):
      base.KeyedModelHandler(mhs)

    mhs = [
        base.KeyModelMapping([0],
                             FakeModelHandler(
                                 state=200, multi_process_shared=True)),
        base.KeyModelMapping([0, 1, 2, 3],
                             FakeModelHandler(multi_process_shared=True))
    ]
    with self.assertRaises(ValueError):
      base.KeyedModelHandler(mhs)

    mhs = [
        base.KeyModelMapping([],
                             FakeModelHandler(
                                 state=200, multi_process_shared=True)),
        base.KeyModelMapping([0, 1, 2, 3],
                             FakeModelHandler(multi_process_shared=True))
    ]
    with self.assertRaises(ValueError):
      base.KeyedModelHandler(mhs)

  def test_keyed_model_handler_get_num_bytes(self):
    mh = base.KeyedModelHandler(FakeModelHandler(num_bytes_per_element=10))
    batch = [('key1', 1), ('key2', 2), ('key1', 3)]
    expected = len(pickle.dumps(('key1', 'key2', 'key1'))) + 30
    actual = mh.get_num_bytes(batch)
    self.assertEqual(expected, actual)

  def test_keyed_model_handler_multiple_models_get_num_bytes(self):
    mhs = [
        base.KeyModelMapping(['key1'],
                             FakeModelHandler(num_bytes_per_element=10)),
        base.KeyModelMapping(['key2'],
                             FakeModelHandler(num_bytes_per_element=20))
    ]
    mh = base.KeyedModelHandler(mhs)
    batch = [('key1', 1), ('key2', 2), ('key1', 3)]
    expected = len(pickle.dumps(('key1', 'key2', 'key1'))) + 40
    actual = mh.get_num_bytes(batch)
    self.assertEqual(expected, actual)

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

  def test_run_inference_prebatched(self):
    with TestPipeline() as pipeline:
      examples = [[1, 5], [3, 10]]
      expected = [int(example) + 1 for batch in examples for example in batch]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(FakeModelHandler().with_no_batching())
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

  def test_run_inference_timeout_on_load_dlq(self):
    with TestPipeline() as pipeline:
      examples = [1, 2]
      expected_good = []
      expected_bad = [1, 2]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      main, other = pcoll | base.RunInference(
          FakeSlowModelHandler(10)).with_exception_handling(timeout=2)
      assert_that(main, equal_to(expected_good), label='assert:inferences')

      # bad.failed_inferences will be in form [batch[elements], error].
      # Just pull out bad element.
      bad_without_error = other.failed_inferences | beam.Map(lambda x: x[0][0])
      assert_that(
          bad_without_error, equal_to(expected_bad), label='assert:failures')

  def test_run_inference_timeout_on_inference_dlq(self):
    with TestPipeline() as pipeline:
      examples = [10, 11]
      expected_good = []
      expected_bad = [10, 11]
      pcoll = pipeline | 'start' >> beam.Create(examples)
      main, other = pcoll | base.RunInference(
          FakeSlowModelHandler(0)).with_exception_handling(timeout=5)
      assert_that(main, equal_to(expected_good), label='assert:inferences')

      # bad.failed_inferences will be in form [batch[elements], error].
      # Just pull out bad element.
      bad_without_error = other.failed_inferences | beam.Map(lambda x: x[0][0])
      assert_that(
          bad_without_error, equal_to(expected_bad), label='assert:failures')

  def test_run_inference_timeout_not_hit(self):
    with TestPipeline() as pipeline:
      examples = [1, 2]
      expected_good = [1, 2]
      expected_bad = []
      pcoll = pipeline | 'start' >> beam.Create(examples)
      main, other = pcoll | base.RunInference(
          FakeSlowModelHandler(3)).with_exception_handling(timeout=500)
      assert_that(main, equal_to(expected_good), label='assert:inferences')

      # bad.failed_inferences will be in form [batch[elements], error].
      # Just pull out bad element.
      bad_without_error = other.failed_inferences | beam.Map(lambda x: x[0][0])
      assert_that(
          bad_without_error, equal_to(expected_bad), label='assert:failures')

  @unittest.skipIf(
      sys.platform == "win32" or sys.version_info < (3, 11),
      "This test relies on the __del__ lifecycle method, but __del__ does " +
      "not get invoked in the same way on older versions of Python or on " +
      "windows, breaking this test. See " +
      "github.com/python/cpython/issues/87950#issuecomment-1807570983 " +
      "for example.")
  def test_run_inference_timeout_does_garbage_collection(self):
    with tempfile.TemporaryDirectory() as tmp_dirname:
      tmp_path = os.path.join(tmp_dirname, 'tmp_filename')
      expected_file_contents = 'Deleted FakeSlowModel'
      with TestPipeline() as pipeline:
        # Start with bad example which gets timed out.
        # Then provide plenty of time for GC to happen.
        examples = [20] + [1] * 15
        expected_good = [1] * 15
        expected_bad = [20]
        pcoll = pipeline | 'start' >> beam.Create(examples)
        main, other = pcoll | base.RunInference(
            FakeSlowModelHandler(
              0, True, tmp_path)).with_exception_handling(timeout=5)

        assert_that(main, equal_to(expected_good), label='assert:inferences')

        # # bad.failed_inferences will be in form [batch[elements], error].
        # # Just pull out bad element.
        bad_without_error = other.failed_inferences | beam.Map(
            lambda x: x[0][0])
        assert_that(
            bad_without_error, equal_to(expected_bad), label='assert:failures')

      with open(tmp_path) as f:
        s = f.read()
        self.assertEqual(s, expected_file_contents)

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

  def test_run_inference_side_input_in_batch_per_key_models(self):
    first_ts = math.floor(time.time()) - 30
    interval = 7

    sample_main_input_elements = ([
        ('key1', first_ts - 2),
        ('key2', first_ts + 1),
        ('key2', first_ts + 8),
        ('key1', first_ts + 15),
        ('key2', first_ts + 22),
        ('key1', first_ts + 29),
    ])

    sample_side_input_elements = [
        (
            first_ts + 1,
            [
                base.KeyModelPathMapping(
                    keys=['key1'], update_path='fake_model_id_default'),
                base.KeyModelPathMapping(
                    keys=['key2'], update_path='fake_model_id_default')
            ]),
        # if model_id is empty string, we use the default model
        # handler model URI.
        (
            first_ts + 8,
            [
                base.KeyModelPathMapping(
                    keys=['key1'], update_path='fake_model_id_1'),
                base.KeyModelPathMapping(
                    keys=['key2'], update_path='fake_model_id_default')
            ]),
        (
            first_ts + 15,
            [
                base.KeyModelPathMapping(
                    keys=['key1'], update_path='fake_model_id_1'),
                base.KeyModelPathMapping(
                    keys=['key2'], update_path='fake_model_id_2')
            ]),
    ]

    model_handler = base.KeyedModelHandler([
        base.KeyModelMapping(['key1'],
                             FakeModelHandlerReturnsPredictionResult(
                                 multi_process_shared=True, state=True)),
        base.KeyModelMapping(['key2'],
                             FakeModelHandlerReturnsPredictionResult(
                                 multi_process_shared=True, state=True))
    ])

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
          | "MapTimeStamp" >> beam.Map(lambda x: TimestampedValue(x, x[1]))
          | "ApplyWindow" >> beam.WindowInto(window.FixedWindows(interval))
          | "RunInference" >> base.RunInference(
              model_handler, model_metadata_pcoll=side_input)
          | beam.Map(lambda x: x[1]))

      expected_model_id_order = [
          'fake_model_id_default',
          'fake_model_id_default',
          'fake_model_id_default',
          'fake_model_id_1',
          'fake_model_id_2',
          'fake_model_id_1',
      ]

      expected_inferences = [
          0,
          0,
          1,
          0,
          0,
          1,
      ]

      expected_result = [
          base.PredictionResult(
              example=sample_main_input_elements[i][1],
              inference=expected_inferences[i],
              model_id=expected_model_id_order[i])
          for i in range(len(expected_inferences))
      ]

      assert_that(result_pcoll, equal_to(expected_result))

  def test_run_inference_side_input_in_batch_per_key_models_split_cohort(self):
    first_ts = math.floor(time.time()) - 30
    interval = 7

    sample_main_input_elements = ([
        ('key1', first_ts - 2),
        ('key2', first_ts + 1),
        ('key1', first_ts + 8),
        ('key2', first_ts + 15),
        ('key1', first_ts + 22),
    ])

    sample_side_input_elements = [
        (
            first_ts + 1,
            [
                base.KeyModelPathMapping(
                    keys=['key1', 'key2'], update_path='fake_model_id_default')
            ]),
        # if model_id is empty string, we use the default model
        # handler model URI.
        (
            first_ts + 8,
            [
                base.KeyModelPathMapping(
                    keys=['key1'], update_path='fake_model_id_1'),
                base.KeyModelPathMapping(
                    keys=['key2'], update_path='fake_model_id_default')
            ]),
        (
            first_ts + 15,
            [
                base.KeyModelPathMapping(
                    keys=['key1'], update_path='fake_model_id_1'),
                base.KeyModelPathMapping(
                    keys=['key2'], update_path='fake_model_id_2')
            ]),
    ]

    model_handler = base.KeyedModelHandler([
        base.KeyModelMapping(
            ['key1', 'key2'],
            FakeModelHandlerReturnsPredictionResult(multi_process_shared=True))
    ])

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
          | "MapTimeStamp" >> beam.Map(lambda x: TimestampedValue(x, x[1]))
          | "ApplyWindow" >> beam.WindowInto(window.FixedWindows(interval))
          | "RunInference" >> base.RunInference(
              model_handler, model_metadata_pcoll=side_input)
          | beam.Map(lambda x: x[1]))

      expected_model_id_order = [
          'fake_model_id_default',
          'fake_model_id_default',
          'fake_model_id_1',
          'fake_model_id_2',
          'fake_model_id_1'
      ]
      expected_result = [
          base.PredictionResult(
              example=sample_main_input_elements[i][1],
              inference=sample_main_input_elements[i][1] + 1,
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
    tag1 = mm.load('key1').model_tag
    # Use bad_mh's load function to make sure we're actually loading the
    # version already stored
    bad_mh = FakeModelHandler(state=100)
    model1 = multi_process_shared.MultiProcessShared(
        bad_mh.load_model, tag=tag1).acquire()
    self.assertEqual(1, model1.predict(10))

    tag2 = mm.load('key2').model_tag
    tag3 = mm.load('key3').model_tag
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
    mm = base._ModelManager(mh_map=mhs)
    mm.increment_max_models(2)
    tag1 = mm.load('key1').model_tag
    sh1 = multi_process_shared.MultiProcessShared(mh1.load_model, tag=tag1)
    model1 = sh1.acquire()
    self.assertEqual(1, model1.predict(10))
    model1.increment_state(5)

    tag2 = mm.load('key2').model_tag
    tag3 = mm.load('key3').model_tag
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

  def test_model_manager_evicts_models_after_update(self):
    mh1 = FakeModelHandler(state=1)
    mhs = {'key1': mh1}
    mm = base._ModelManager(mh_map=mhs)
    tag1 = mm.load('key1').model_tag
    sh1 = multi_process_shared.MultiProcessShared(mh1.load_model, tag=tag1)
    model1 = sh1.acquire()
    self.assertEqual(1, model1.predict(10))
    model1.increment_state(5)
    self.assertEqual(6, model1.predict(10))
    mm.update_model_handler('key1', 'fake/path', 'key1')
    self.assertEqual(6, model1.predict(10))
    sh1.release(model1)

    tag1 = mm.load('key1').model_tag
    sh1 = multi_process_shared.MultiProcessShared(mh1.load_model, tag=tag1)
    model1 = sh1.acquire()
    self.assertEqual(1, model1.predict(10))
    model1.increment_state(5)
    self.assertEqual(6, model1.predict(10))
    sh1.release(model1)

    # Shouldn't evict if path is the same as last update
    mm.update_model_handler('key1', 'fake/path', 'key1')
    tag1 = mm.load('key1').model_tag
    sh1 = multi_process_shared.MultiProcessShared(mh1.load_model, tag=tag1)
    model1 = sh1.acquire()
    self.assertEqual(6, model1.predict(10))
    sh1.release(model1)

  def test_model_manager_evicts_correct_num_of_models_after_being_incremented(
      self):
    mh1 = FakeModelHandler(state=1)
    mh2 = FakeModelHandler(state=2)
    mh3 = FakeModelHandler(state=3)
    mhs = {'key1': mh1, 'key2': mh2, 'key3': mh3}
    mm = base._ModelManager(mh_map=mhs)
    mm.increment_max_models(1)
    mm.increment_max_models(1)
    tag1 = mm.load('key1').model_tag
    sh1 = multi_process_shared.MultiProcessShared(mh1.load_model, tag=tag1)
    model1 = sh1.acquire()
    self.assertEqual(1, model1.predict(10))
    model1.increment_state(5)
    self.assertEqual(6, model1.predict(10))
    sh1.release(model1)

    tag2 = mm.load('key2').model_tag
    tag3 = mm.load('key3').model_tag
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

  def test_run_inference_loads_different_models(self):
    mh1 = FakeModelHandler(incrementing=True, min_batch_size=3)
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'start' >> beam.Create([1, 2, 3])
      actual = (
          pcoll
          | 'ri1' >> base.RunInference(mh1)
          | 'ri2' >> base.RunInference(mh1))
      assert_that(actual, equal_to([1, 2, 3]), label='assert:inferences')

  def test_run_inference_loads_different_models_multi_process_shared(self):
    mh1 = FakeModelHandler(
        incrementing=True, min_batch_size=3, multi_process_shared=True)
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'start' >> beam.Create([1, 2, 3])
      actual = (
          pcoll
          | 'ri1' >> base.RunInference(mh1)
          | 'ri2' >> base.RunInference(mh1))
      assert_that(actual, equal_to([1, 2, 3]), label='assert:inferences')

  def test_runinference_loads_same_model_with_identifier_multi_process_shared(
      self):
    mh1 = FakeModelHandler(
        incrementing=True, min_batch_size=3, multi_process_shared=True)
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'start' >> beam.Create([1, 2, 3])
      actual = (
          pcoll
          | 'ri1' >> base.RunInference(
              mh1,
              model_identifier='same_model_with_identifier_multi_process_shared'
          )
          | 'ri2' >> base.RunInference(
              mh1,
              model_identifier='same_model_with_identifier_multi_process_shared'
          ))
      assert_that(actual, equal_to([4, 5, 6]), label='assert:inferences')

  def test_run_inference_watch_file_pattern_side_input_label(self):
    pipeline = TestPipeline()
    # label of the WatchPattern transform.
    side_input_str = 'WatchFilePattern/ApplyGlobalWindow'
    from apache_beam.ml.inference.utils import WatchFilePattern
    file_pattern_side_input = (
        pipeline
        | 'WatchFilePattern' >> WatchFilePattern(file_pattern='fake/path/*'))
    pcoll = pipeline | 'start' >> beam.Create([1, 2, 3])
    result_pcoll = pcoll | base.RunInference(
        FakeModelHandler(), model_metadata_pcoll=file_pattern_side_input)
    assert side_input_str in str(result_pcoll.producer.side_inputs[0])

  def test_run_inference_watch_file_pattern_keyword_arg_side_input_label(self):
    # label of the WatchPattern transform.
    side_input_str = 'WatchFilePattern/ApplyGlobalWindow'
    pipeline = TestPipeline()
    pcoll = pipeline | 'start' >> beam.Create([1, 2, 3])
    result_pcoll = pcoll | base.RunInference(
        FakeModelHandler(), watch_model_pattern='fake/path/*')
    assert side_input_str in str(result_pcoll.producer.side_inputs[0])

  def test_model_status_provides_valid_tags(self):
    ms = base._ModelStatus(False)

    self.assertTrue(ms.is_valid_tag('tag1'))
    self.assertTrue(ms.is_valid_tag('tag2'))

    self.assertEqual('tag1', ms.get_valid_tag('tag1'))
    self.assertEqual('tag2', ms.get_valid_tag('tag2'))

    self.assertTrue(ms.is_valid_tag('tag1'))
    self.assertTrue(ms.is_valid_tag('tag2'))

    ms.try_mark_current_model_invalid(0)

    self.assertFalse(ms.is_valid_tag('tag1'))
    self.assertFalse(ms.is_valid_tag('tag2'))

    self.assertEqual('tag1_reload_1', ms.get_valid_tag('tag1'))
    self.assertEqual('tag2_reload_1', ms.get_valid_tag('tag2'))

    self.assertTrue(ms.is_valid_tag('tag1_reload_1'))
    self.assertTrue(ms.is_valid_tag('tag2_reload_1'))

    ms.try_mark_current_model_invalid(0)

    self.assertFalse(ms.is_valid_tag('tag1'))
    self.assertFalse(ms.is_valid_tag('tag2'))
    self.assertFalse(ms.is_valid_tag('tag1_reload_1'))
    self.assertFalse(ms.is_valid_tag('tag2_reload_1'))

    self.assertEqual('tag1_reload_2', ms.get_valid_tag('tag1'))
    self.assertEqual('tag2_reload_2', ms.get_valid_tag('tag2'))

    self.assertTrue(ms.is_valid_tag('tag1_reload_2'))
    self.assertTrue(ms.is_valid_tag('tag2_reload_2'))

  def test_model_status_provides_valid_garbage_collection(self):
    ms = base._ModelStatus(True)

    self.assertTrue(ms.is_valid_tag('mspvgc_tag1'))
    self.assertTrue(ms.is_valid_tag('mspvgc_tag2'))

    ms.try_mark_current_model_invalid(1)

    tags = ms.get_tags_for_garbage_collection()

    self.assertEqual(0, len(tags))

    time.sleep(4)

    ms.try_mark_current_model_invalid(1)

    time.sleep(4)

    self.assertTrue(ms.is_valid_tag('mspvgc_tag3'))

    tags = ms.get_tags_for_garbage_collection()

    self.assertEqual(2, len(tags))
    self.assertTrue('mspvgc_tag1' in tags)
    self.assertTrue('mspvgc_tag2' in tags)

    ms.try_mark_current_model_invalid(0)

    tags = ms.get_tags_for_garbage_collection()

    self.assertEqual(3, len(tags))
    self.assertTrue('mspvgc_tag1' in tags)
    self.assertTrue('mspvgc_tag2' in tags)
    self.assertTrue('mspvgc_tag3' in tags)

    ms.mark_tags_deleted(set(['mspvgc_tag1', 'mspvgc_tag2']))
    tags = ms.get_tags_for_garbage_collection()

    self.assertEqual(['mspvgc_tag3'], tags)

    ms.mark_tags_deleted(set(['mspvgc_tag1']))
    tags = ms.get_tags_for_garbage_collection()

    self.assertEqual(1, len(tags))
    self.assertEqual('mspvgc_tag3', tags[0])

    ms.mark_tags_deleted(set(['mspvgc_tag3']))
    tags = ms.get_tags_for_garbage_collection()

    self.assertEqual(0, len(tags))


if __name__ == '__main__':
  unittest.main()
