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

"""An extensible run inference transform."""

import logging
import os
import pickle
import platform
import sys
import time
from typing import Any
from typing import Iterable
from typing import Tuple

import apache_beam as beam
from apache_beam.utils import shared

try:
  # pylint: disable=g-import-not-at-top
  import resource
except ImportError:
  resource = None

_MILLISECOND_TO_MICROSECOND = 1000
_MICROSECOND_TO_NANOSECOND = 1000
_SECOND_TO_MICROSECOND = 1000000


class InferenceRunner():
  """Implements running inferences for a framework."""
  def run_inference(self, batch: Any, model: Any) -> Iterable[Any]:
    """Runs inferences on a batch of examples and returns an Iterable of Predictions."""
    raise NotImplementedError(type(self))

  def get_num_bytes(self, batch: Any) -> int:
    """Returns the number of bytes of data for a batch."""
    return len(pickle.dumps(batch))

  def get_metrics_namespace(self) -> str:
    """Returns a namespace for metrics collected by the RunInference transform."""
    return 'RunInference'


class ModelLoader():
  """Has the ability to load an ML model."""
  def load_model(self) -> Any:
    """Loads and initializes a model for processing."""
    raise NotImplementedError(type(self))

  def get_inference_runner(self) -> InferenceRunner:
    """Returns an implementation of InferenceRunner for this model."""
    raise NotImplementedError(type(self))


def _unbatch(maybe_keyed_batches: Tuple[Any, Any]):
  keys, results = maybe_keyed_batches
  if keys:
    return zip(keys, results)
  else:
    return results


class RunInference(beam.PTransform):
  """An extensible transform for running inferences."""
  def __init__(self, model_loader: ModelLoader, clock=None):
    self._model_loader = model_loader
    self._clock = clock

  # TODO(BEAM-14208): Add batch_size back off in the case there
  # are functional reasons large batch sizes cannot be handled.
  def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
    return (
        pcoll
        # TODO(BEAM-14044): Hook into the batching DoFn APIs.
        | beam.BatchElements()
        | beam.ParDo(
            RunInferenceDoFn(shared.Shared(), self._model_loader, self._clock))
        | beam.FlatMap(_unbatch))


class MetricsCollector:
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
    self.load_model_latency_milli_secs_cache = None
    self.model_byte_size_cache = None

  def update_metrics_with_cache(self):
    if self.load_model_latency_milli_secs_cache is not None:
      self._load_model_latency_milli_secs.update(
          self.load_model_latency_milli_secs_cache)
      self.load_model_latency_milli_secs_cache = None
    if self.model_byte_size_cache is not None:
      self._model_byte_size.update(self.model_byte_size_cache)
      self.model_byte_size_cache = None

  def update(
      self,
      examples_count: int,
      examples_byte_size: int,
      latency_micro_secs: int):
    self._inference_batch_latency_micro_secs.update(latency_micro_secs)
    self._inference_counter.inc(examples_count)
    self._inference_request_batch_size.update(examples_count)
    self._inference_request_batch_byte_size.update(examples_byte_size)


class RunInferenceDoFn(beam.DoFn):
  """A DoFn implementation generic to frameworks."""
  def __init__(
      self,
      shared_handle: shared.Shared,
      model_loader: ModelLoader,
      clock=None):
    self._model_loader = model_loader
    self._inference_runner = model_loader.get_inference_runner()
    self._shared_model_handle = shared_handle
    self._metrics_collector = MetricsCollector(
        self._inference_runner.get_metrics_namespace())
    self._clock = clock
    if not clock:
      self._clock = _ClockFactory.make_clock()
    self._model = None

  def _load_model(self):
    def load():
      """Function for constructing shared LoadedModel."""
      memory_before = _get_current_process_memory_in_bytes()
      start_time = self._clock.get_current_time_in_microseconds()
      model = self._model_loader.load_model()
      end_time = self._clock.get_current_time_in_microseconds()
      memory_after = _get_current_process_memory_in_bytes()
      self._metrics_collector.load_model_latency_milli_secs_cache = (
          (end_time - start_time) / _MILLISECOND_TO_MICROSECOND)
      self._metrics_collector.model_byte_size_cache = (
          memory_after - memory_before)
      return model

    # TODO(BEAM-14207): Investigate releasing model.
    return self._shared_model_handle.acquire(load)

  def setup(self):
    self._model = self._load_model()

  def process(self, batch):
    # Process supports both keyed data, and example only data.
    # First keys and samples are separated (if there are keys)
    has_keys = isinstance(batch[0], tuple)
    if has_keys:
      examples = [example for _, example in batch]
      keys = [key for key, _ in batch]
    else:
      examples = batch
      keys = None

    start_time = self._clock.get_current_time_in_microseconds()
    result_generator = self._inference_runner.run_inference(
        examples, self._model)
    predictions = list(result_generator)

    inference_latency = self._clock.get_current_time_in_microseconds(
    ) - start_time
    num_bytes = self._inference_runner.get_num_bytes(examples)
    num_elements = len(batch)
    self._metrics_collector.update(num_elements, num_bytes, inference_latency)

    # Keys are recombined with predictions in the RunInference PTransform.
    yield keys, predictions

  def finish_bundle(self):
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


def _is_windows() -> bool:
  return platform.system() == 'Windows' or os.name == 'nt'


def _is_cygwin() -> bool:
  return platform.system().startswith('CYGWIN_NT')


class Clock(object):
  def get_current_time_in_microseconds(self) -> int:
    return int(time.time() * _SECOND_TO_MICROSECOND)


class _FineGrainedClock(Clock):
  def get_current_time_in_microseconds(self) -> int:
    return int(
        time.clock_gettime_ns(time.CLOCK_REALTIME) /  # pytype: disable=module-attr
        _MICROSECOND_TO_NANOSECOND)


class _ClockFactory(object):
  @staticmethod
  def make_clock() -> Clock:
    if (hasattr(time, 'clock_gettime_ns') and not _is_windows() and
        not _is_cygwin()):
      return _FineGrainedClock()
    return Clock()
