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

"""An extensible run inference transform.

Users of this module can extend the ModelLoader class for any MLframework. Then
pass their extended ModelLoader object into RunInference to create a
RunInference Beam transform for that framework.

The transform will handle standard inference functionality like metric
collection, sharing model between threads and batching elements.

Note: This module is still actively being developed and users should have
no expectation that these interfaces will not change.
"""

import logging
import pickle
import sys
import time
from typing import Any
from typing import Generic
from typing import Iterable
from typing import List
from typing import Mapping
from typing import TypeVar

import apache_beam as beam
from apache_beam.utils import shared

try:
  # pylint: disable=wrong-import-order, wrong-import-position
  import resource
except ImportError:
  resource = None  # type: ignore[assignment]

_NANOSECOND_TO_MILLISECOND = 1_000_000
_NANOSECOND_TO_MICROSECOND = 1_000

ModelT = TypeVar('ModelT')
ExampleT = TypeVar('ExampleT')
PredictionT = TypeVar('PredictionT')


def _to_milliseconds(time_ns: int) -> int:
  return int(time_ns / _NANOSECOND_TO_MILLISECOND)


def _to_microseconds(time_ns: int) -> int:
  return int(time_ns / _NANOSECOND_TO_MICROSECOND)


class InferenceRunner(Generic[ExampleT, PredictionT, ModelT]):
  """Implements running inferences for a framework."""
  def run_inference(self, batch: List[ExampleT], model: ModelT,
                    **kwargs) -> Iterable[PredictionT]:
    """Runs inferences on a batch of examples and
    returns an Iterable of Predictions."""
    raise NotImplementedError(type(self))

  def get_num_bytes(self, batch: List[ExampleT]) -> int:
    """Returns the number of bytes of data for a batch."""
    return len(pickle.dumps(batch))

  def get_metrics_namespace(self) -> str:
    """Returns a namespace for metrics collected by RunInference transform."""
    return 'RunInference'


class ModelLoader(Generic[ExampleT, PredictionT, ModelT]):
  """Has the ability to load an ML model."""
  def load_model(self) -> ModelT:
    """Loads and initializes a model for processing."""
    raise NotImplementedError(type(self))

  def get_inference_runner(
      self) -> InferenceRunner[ExampleT, PredictionT, ModelT]:
    """Returns an implementation of InferenceRunner for this model."""
    raise NotImplementedError(type(self))

  def get_resource_hints(self) -> dict:
    """Returns resource hints for the transform."""
    return {}

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    """Returns kwargs suitable for beam.BatchElements."""
    return {}


class RunInference(beam.PTransform[beam.PCollection[ExampleT],
                                   beam.PCollection[PredictionT]]):
  """An extensible transform for running inferences.
  Args:
      model_loader: An implementation of ModelLoader.
      clock: A clock implementing get_current_time_in_microseconds.
  """
  def __init__(
      self,
      model_loader: ModelLoader[ExampleT, PredictionT, Any],
      clock=time,
      **kwargs):
    self._model_loader = model_loader
    self._kwargs = kwargs
    self._clock = clock

  # TODO(BEAM-14208): Add batch_size back off in the case there
  # are functional reasons large batch sizes cannot be handled.
  def expand(
      self, pcoll: beam.PCollection[ExampleT]) -> beam.PCollection[PredictionT]:
    resource_hints = self._model_loader.get_resource_hints()
    return (
        pcoll
        # TODO(BEAM-14044): Hook into the batching DoFn APIs.
        | beam.BatchElements(**self._model_loader.batch_elements_kwargs())
        | (
            beam.ParDo(
                _RunInferenceDoFn(self._model_loader, self._clock),
                **self._kwargs).with_resource_hints(**resource_hints)))


class _MetricsCollector:
  """A metrics collector that tracks ML related performance and memory usage."""
  def __init__(self, namespace: str):
    # Metrics
    self._inference_counter = beam.metrics.Metrics.counter(
        namespace, 'num_inferences')
    self._inference_request_batch_size = beam.metrics.Metrics.distribution(
        namespace, 'inference_request_batch_size')
    self._inference_request_batch_byte_size = (
        beam.metrics.Metrics.distribution(
            namespace, 'inference_request_batch_byte_size'))
    # Batch inference latency in microseconds.
    self._inference_batch_latency_micro_secs = (
        beam.metrics.Metrics.distribution(
            namespace, 'inference_batch_latency_micro_secs'))
    self._model_byte_size = beam.metrics.Metrics.distribution(
        namespace, 'model_byte_size')
    # Model load latency in milliseconds.
    self._load_model_latency_milli_secs = beam.metrics.Metrics.distribution(
        namespace, 'load_model_latency_milli_secs')

    # Metrics cache
    self._load_model_latency_milli_secs_cache = None
    self._model_byte_size_cache = None

  def update_metrics_with_cache(self):
    if self._load_model_latency_milli_secs_cache is not None:
      self._load_model_latency_milli_secs.update(
          self._load_model_latency_milli_secs_cache)
      self._load_model_latency_milli_secs_cache = None
    if self._model_byte_size_cache is not None:
      self._model_byte_size.update(self._model_byte_size_cache)
      self._model_byte_size_cache = None

  def cache_load_model_metrics(self, load_model_latency_ms, model_byte_size):
    self._load_model_latency_milli_secs_cache = load_model_latency_ms
    self._model_byte_size_cache = model_byte_size

  def update(
      self,
      examples_count: int,
      examples_byte_size: int,
      latency_micro_secs: int):
    self._inference_batch_latency_micro_secs.update(latency_micro_secs)
    self._inference_counter.inc(examples_count)
    self._inference_request_batch_size.update(examples_count)
    self._inference_request_batch_byte_size.update(examples_byte_size)


class _RunInferenceDoFn(beam.DoFn, Generic[ExampleT, PredictionT]):
  """A DoFn implementation generic to frameworks."""
  def __init__(
      self, model_loader: ModelLoader[ExampleT, PredictionT, Any], clock):
    self._model_loader = model_loader
    self._shared_model_handle = shared.Shared()
    self._clock = clock
    self._model = None

  def _load_model(self):
    def load():
      """Function for constructing shared LoadedModel."""
      memory_before = _get_current_process_memory_in_bytes()
      start_time = _to_milliseconds(self._clock.time_ns())
      model = self._model_loader.load_model()
      end_time = _to_milliseconds(self._clock.time_ns())
      memory_after = _get_current_process_memory_in_bytes()
      load_model_latency_ms = end_time - start_time
      model_byte_size = memory_after - memory_before
      self._metrics_collector.cache_load_model_metrics(
          load_model_latency_ms, model_byte_size)
      return model

    # TODO(BEAM-14207): Investigate releasing model.
    return self._shared_model_handle.acquire(load)

  def setup(self):
    self._inference_runner = self._model_loader.get_inference_runner()
    self._metrics_collector = _MetricsCollector(
        self._inference_runner.get_metrics_namespace())
    self._model = self._load_model()

  def process(self, batch, **kwargs):
    # Process supports both keyed data, and example only data.
    # First keys and samples are separated (if there are keys)
    has_keys = isinstance(batch[0], tuple)
    if has_keys:
      examples = [example for _, example in batch]
      keys = [key for key, _ in batch]
    else:
      examples = batch
      keys = None

    start_time = _to_microseconds(self._clock.time_ns())
    result_generator = self._inference_runner.run_inference(
        examples, self._model, **kwargs)
    predictions = list(result_generator)

    end_time = _to_microseconds(self._clock.time_ns())
    inference_latency = end_time - start_time
    num_bytes = self._inference_runner.get_num_bytes(examples)
    num_elements = len(batch)
    self._metrics_collector.update(num_elements, num_bytes, inference_latency)

    # Keys are recombined with predictions in the RunInference PTransform.
    if has_keys:
      yield from zip(keys, predictions)
    else:
      yield from predictions

  def finish_bundle(self):
    # TODO(BEAM-13970): Figure out why there is a cache.
    self._metrics_collector.update_metrics_with_cache()


def _is_darwin() -> bool:
  return sys.platform == 'darwin'


def _get_current_process_memory_in_bytes():
  """Returns memory usage in bytes."""

  if resource is not None:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if _is_darwin():
      return usage
    return usage * 1024
  else:
    logging.warning(
        'Resource module is not available for current platform, '
        'memory usage cannot be fetched.')
  return 0
