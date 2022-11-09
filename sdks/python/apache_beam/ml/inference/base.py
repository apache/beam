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
# TODO: https://github.com/apache/beam/issues/21822
# mypy: ignore-errors

"""An extensible run inference transform.

Users of this module can extend the ModelHandler class for any machine learning
framework. A ModelHandler implementation is a required parameter of
RunInference.

The transform will handle standard inference functionality like metric
collection, sharing model between threads and batching elements.
"""

import logging
import pickle
import sys
import time
from typing import Any
from typing import Dict
from typing import Generic
from typing import Iterable
from typing import Mapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TypeVar
from typing import Union

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
_INPUT_TYPE = TypeVar('_INPUT_TYPE')
_OUTPUT_TYPE = TypeVar('_OUTPUT_TYPE')
KeyT = TypeVar('KeyT')

PredictionResult = NamedTuple(
    'PredictionResult', [
        ('example', _INPUT_TYPE),
        ('inference', _OUTPUT_TYPE),
    ])
PredictionResult.__doc__ = """A NamedTuple containing both input and output
  from the inference."""
PredictionResult.example.__doc__ = """The input example."""
PredictionResult.inference.__doc__ = """Results for the inference on the model
  for the given example."""


def _to_milliseconds(time_ns: int) -> int:
  return int(time_ns / _NANOSECOND_TO_MILLISECOND)


def _to_microseconds(time_ns: int) -> int:
  return int(time_ns / _NANOSECOND_TO_MICROSECOND)


class ModelHandler(Generic[ExampleT, PredictionT, ModelT]):
  """Has the ability to load and apply an ML model."""
  def load_model(self) -> ModelT:
    """Loads and initializes a model for processing."""
    raise NotImplementedError(type(self))

  def run_inference(
      self,
      batch: Sequence[ExampleT],
      model: ModelT,
      inference_args: Optional[Dict[str, Any]] = None) -> Iterable[PredictionT]:
    """Runs inferences on a batch of examples.

    Args:
      batch: A sequence of examples or features.
      model: The model used to make inferences.
      inference_args: Extra arguments for models whose inference call requires
        extra parameters.

    Returns:
      An Iterable of Predictions.
    """
    raise NotImplementedError(type(self))

  def get_num_bytes(self, batch: Sequence[ExampleT]) -> int:
    """
    Returns:
       The number of bytes of data for a batch.
    """
    return len(pickle.dumps(batch))

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by RunInference transform.
    """
    return 'RunInference'

  def get_resource_hints(self) -> dict:
    """
    Returns:
       Resource hints for the transform.
    """
    return {}

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    """
    Returns:
       kwargs suitable for beam.BatchElements.
    """
    return {}

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    """Validates inference_args passed in the inference call.

    Most frameworks do not need extra arguments in their predict() call so the
    default behavior is to error out if inference_args are present.
    """
    if inference_args:
      raise ValueError(
          'inference_args were provided, but should be None because this '
          'framework does not expect extra arguments on inferences.')


class KeyedModelHandler(Generic[KeyT, ExampleT, PredictionT, ModelT],
                        ModelHandler[Tuple[KeyT, ExampleT],
                                     Tuple[KeyT, PredictionT],
                                     ModelT]):
  def __init__(self, unkeyed: ModelHandler[ExampleT, PredictionT, ModelT]):
    """A ModelHandler that takes keyed examples and returns keyed predictions.

    For example, if the original model was used with RunInference to take a
    PCollection[E] to a PCollection[P], this would take a
    PCollection[Tuple[K, E]] to a PCollection[Tuple[K, P]], allowing one to
    associate the outputs with the inputs based on the key.

    Args:
      unkeyed: An implementation of ModelHandler that does not require keys.
    """
    self._unkeyed = unkeyed

  def load_model(self) -> ModelT:
    return self._unkeyed.load_model()

  def run_inference(
      self,
      batch: Sequence[Tuple[KeyT, ExampleT]],
      model: ModelT,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[Tuple[KeyT, PredictionT]]:
    keys, unkeyed_batch = zip(*batch)
    return zip(
        keys, self._unkeyed.run_inference(unkeyed_batch, model, inference_args))

  def get_num_bytes(self, batch: Sequence[Tuple[KeyT, ExampleT]]) -> int:
    keys, unkeyed_batch = zip(*batch)
    return len(pickle.dumps(keys)) + self._unkeyed.get_num_bytes(unkeyed_batch)

  def get_metrics_namespace(self) -> str:
    return self._unkeyed.get_metrics_namespace()

  def get_resource_hints(self):
    return self._unkeyed.get_resource_hints()

  def batch_elements_kwargs(self):
    return self._unkeyed.batch_elements_kwargs()

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    return self._unkeyed.validate_inference_args(inference_args)


class MaybeKeyedModelHandler(Generic[KeyT, ExampleT, PredictionT, ModelT],
                             ModelHandler[Union[ExampleT, Tuple[KeyT,
                                                                ExampleT]],
                                          Union[PredictionT,
                                                Tuple[KeyT, PredictionT]],
                                          ModelT]):
  def __init__(self, unkeyed: ModelHandler[ExampleT, PredictionT, ModelT]):
    """A ModelHandler that takes possibly keyed examples and returns possibly
    keyed predictions.

    For example, if the original model was used with RunInference to take a
    PCollection[E] to a PCollection[P], this would take either PCollection[E]
    to a PCollection[P] or PCollection[Tuple[K, E]] to a
    PCollection[Tuple[K, P]], depending on the whether the elements happen to
    be tuples, allowing one to associate the outputs with the inputs based on
    the key.

    Note that this cannot be used if E happens to be a tuple type.  In addition,
    either all examples should be keyed, or none of them.

    Args:
      unkeyed: An implementation of ModelHandler that does not require keys.
    """
    self._unkeyed = unkeyed

  def load_model(self) -> ModelT:
    return self._unkeyed.load_model()

  def run_inference(
      self,
      batch: Sequence[Union[ExampleT, Tuple[KeyT, ExampleT]]],
      model: ModelT,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Union[Iterable[PredictionT], Iterable[Tuple[KeyT, PredictionT]]]:
    # Really the input should be
    #    Union[Sequence[ExampleT], Sequence[Tuple[KeyT, ExampleT]]]
    # but there's not a good way to express (or check) that.
    if isinstance(batch[0], tuple):
      is_keyed = True
      keys, unkeyed_batch = zip(*batch)  # type: ignore[arg-type]
    else:
      is_keyed = False
      unkeyed_batch = batch  # type: ignore[assignment]
    unkeyed_results = self._unkeyed.run_inference(
        unkeyed_batch, model, inference_args)
    if is_keyed:
      return zip(keys, unkeyed_results)
    else:
      return unkeyed_results

  def get_num_bytes(
      self, batch: Sequence[Union[ExampleT, Tuple[KeyT, ExampleT]]]) -> int:
    # MyPy can't follow the branching logic.
    if isinstance(batch[0], tuple):
      keys, unkeyed_batch = zip(*batch)  # type: ignore[arg-type]
      return len(
          pickle.dumps(keys)) + self._unkeyed.get_num_bytes(unkeyed_batch)
    else:
      return self._unkeyed.get_num_bytes(batch)  # type: ignore[arg-type]

  def get_metrics_namespace(self) -> str:
    return self._unkeyed.get_metrics_namespace()

  def get_resource_hints(self):
    return self._unkeyed.get_resource_hints()

  def batch_elements_kwargs(self):
    return self._unkeyed.batch_elements_kwargs()

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    return self._unkeyed.validate_inference_args(inference_args)


class RunInference(beam.PTransform[beam.PCollection[ExampleT],
                                   beam.PCollection[PredictionT]]):
  def __init__(
      self,
      model_handler: ModelHandler[ExampleT, PredictionT, Any],
      clock=time,
      inference_args: Optional[Dict[str, Any]] = None,
      metrics_namespace: Optional[str] = None):
    """A transform that takes a PCollection of examples (or features) to be used
    on an ML model. It will then output inferences (or predictions) for those
    examples in a PCollection of PredictionResults, containing the input
    examples and output inferences.

    Models for supported frameworks can be loaded via a URI. Supported services
    can also be used.

    This transform attempts to batch examples using the beam.BatchElements
    transform. Batching may be configured using the ModelHandler.

    Args:
        model_handler: An implementation of ModelHandler.
        clock: A clock implementing time_ns. *Used for unit testing.*
        inference_args: Extra arguments for models whose inference call requires
          extra parameters.
        metrics_namespace: Namespace of the transform to collect metrics.
    """
    self._model_handler = model_handler
    self._inference_args = inference_args
    self._clock = clock
    self._metrics_namespace = metrics_namespace

  # TODO(BEAM-14046): Add and link to help documentation.
  @classmethod
  def from_callable(cls, model_handler_provider, **kwargs):
    """Multi-language friendly constructor.

    This constructor can be used with fully_qualified_named_transform to
    initialize RunInference transform from PythonCallableSource provided
    by foreign SDKs.

    Args:
      model_handler_provider: A callable object that returns ModelHandler.
      kwargs: Keyword arguments for model_handler_provider.
    """
    return cls(model_handler_provider(**kwargs))

  # TODO(https://github.com/apache/beam/issues/21447): Add batch_size back off
  # in the case there are functional reasons large batch sizes cannot be
  # handled.
  def expand(
      self, pcoll: beam.PCollection[ExampleT]) -> beam.PCollection[PredictionT]:
    self._model_handler.validate_inference_args(self._inference_args)
    resource_hints = self._model_handler.get_resource_hints()
    return (
        pcoll
        # TODO(https://github.com/apache/beam/issues/21440): Hook into the
        # batching DoFn APIs.
        | beam.BatchElements(**self._model_handler.batch_elements_kwargs())
        | 'BeamML_RunInference' >> (
            beam.ParDo(
                _RunInferenceDoFn(
                    self._model_handler, self._clock, self._metrics_namespace),
                self._inference_args).with_resource_hints(**resource_hints)))


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
  def __init__(
      self,
      model_handler: ModelHandler[ExampleT, PredictionT, Any],
      clock,
      metrics_namespace):
    """A DoFn implementation generic to frameworks.

      Args:
        model_handler: An implementation of ModelHandler.
        clock: A clock implementing time_ns. *Used for unit testing.*
        metrics_namespace: Namespace of the transform to collect metrics.
    """
    self._model_handler = model_handler
    self._shared_model_handle = shared.Shared()
    self._clock = clock
    self._model = None
    self._metrics_namespace = metrics_namespace

  def _load_model(self):
    def load():
      """Function for constructing shared LoadedModel."""
      memory_before = _get_current_process_memory_in_bytes()
      start_time = _to_milliseconds(self._clock.time_ns())
      model = self._model_handler.load_model()
      end_time = _to_milliseconds(self._clock.time_ns())
      memory_after = _get_current_process_memory_in_bytes()
      load_model_latency_ms = end_time - start_time
      model_byte_size = memory_after - memory_before
      self._metrics_collector.cache_load_model_metrics(
          load_model_latency_ms, model_byte_size)
      return model

    # TODO(https://github.com/apache/beam/issues/21443): Investigate releasing
    # model.
    return self._shared_model_handle.acquire(load)

  def setup(self):
    metrics_namespace = (
        self._metrics_namespace) if self._metrics_namespace else (
            self._model_handler.get_metrics_namespace())
    self._metrics_collector = _MetricsCollector(metrics_namespace)
    self._model = self._load_model()

  def process(self, batch, inference_args):
    start_time = _to_microseconds(self._clock.time_ns())
    result_generator = self._model_handler.run_inference(
        batch, self._model, inference_args)
    predictions = list(result_generator)

    end_time = _to_microseconds(self._clock.time_ns())
    inference_latency = end_time - start_time
    num_bytes = self._model_handler.get_num_bytes(batch)
    num_elements = len(batch)
    self._metrics_collector.update(num_elements, num_bytes, inference_latency)

    return predictions

  def finish_bundle(self):
    # TODO(https://github.com/apache/beam/issues/21435): Figure out why there
    # is a cache.
    self._metrics_collector.update_metrics_with_cache()


def _is_darwin() -> bool:
  return sys.platform == 'darwin'


def _get_current_process_memory_in_bytes():
  """
  Returns:
    memory usage in bytes.
  """

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
