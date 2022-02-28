from dataclasses import dataclass

from typing import Any
from typing import Generic
from typing import Iterable
from typing import TypeVar

import apache_beam as beam
import resource
import sys
import logging

@dataclass
class PredictionResult:
  example: Any
  inference: Any


#  metadata: Any
def _unbatch(maybe_keyed_batches):
  keys, results = maybe_keyed_batches
  if keys:
    return zip(keys, results)
  else:
    return results


ExampleType = TypeVar('ExampleType')
InferenceType = TypeVar('InferenceType')

class ModelSpec(Generic[ExampleType, InferenceType]):
  """Defines a model that can load into memory and run inferences."""

  def load_model(self, shared: beam.Shared):
    """Loads an initializes a model for processing."""
    raise NotImplementedError(type(self))

  def run_inference(self, batch: ExampleType) -> InferenceType:
    raise NotImplementedError(type(self))

  def clean_up(self):
    pass

class MetricsCollector:
  """A collector for beam metrics."""

  def __init__(self, namespace: str):
    # Metrics
    self._inference_counter = beam.metrics.Metrics.counter(
        namespace, 'num_inferences')
    self._num_instances = beam.metrics.Metrics.counter(
        namespace, 'num_instances')
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

  def update(self, examples_count: int, examples_byte_size: int,
             latency_micro_secs: int):
    self._inference_batch_latency_micro_secs.update(latency_micro_secs)
    self._num_instances.inc(examples_count)
    self._inference_counter.inc(examples_count)
    self._inference_request_batch_size.update(examples_count)
    self._inference_request_batch_byte_size.update(examples_byte_size)

class RunInferenceDoFn(beam.DoFn):
  def __init__(self, model_spec: ModelSpec[ExampleType, InferenceType]):
    # TODO: Also handle models coming from side inputs.
    self._model_spec = model_spec
    self._shared_model_handle = beam.Shared()

  def _load_model(self):
    def load():
      """Function for constructing shared LoadedModel."""
      self._maybe_register_addon_ops()
      result = tf.compat.v1.Session(graph=tf.compat.v1.Graph())
      memory_before = _get_current_process_memory_in_bytes()
      start_time = self._clock.get_current_time_in_microseconds()
      self._model_spec.load_model()
      end_time = self._clock.get_current_time_in_microseconds()
      memory_after = _get_current_process_memory_in_bytes()
      self._metrics_collector.load_model_latency_milli_secs_cache = (
          (end_time - start_time) / _MILLISECOND_TO_MICROSECOND)
      self._metrics_collector.model_byte_size_cache = (
          memory_after - memory_before)
      return result

    return self._shared_model_handle.acquire(load)


  def setup(self):
    super().setup()
    self._load_model(self._shared)

  def process(self, batch):
    has_keys = isinstance(batch[0], tuple)
    start_time = self._clock.get_current_time_in_microseconds()
    if has_keys:
      examples = batch
    else:
      examples = [example for _, example in batch]
      keys = [key for key, _ in batch]
    inferences = self._model_spec.run_inference(examples)
    inference_latency = self._clock.get_current_time_in_microseconds() - start_time
    num_bytes = sys.getsizeof(batch)
    num_elements = len(batch)
    self._metrics.update(num_elements, sys.getsizeof(batch), inference_latency)
    if has_keys:
      yield [(k, PredictionResult(e, r)) for k, e, r in zip(keys, examples, inferences)]
    else:
      yield [PredictionResult(e, r) for e, r in zip(examples, inferences)]

  def teardown(self):
    self._model_spec.cleanup()


class RunInferenceImpl(beam.PTransform):
  def __init__(self, model_spec: ModelSpec[ExampleType, InferenceType]):
    self._model_spec = model_spec

  def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
    return (
        pcoll
        # TODO: Hook into the batching DoFn APIs.
        | beam.BatchElements(**self._batch_params)
        | beam.ParDo(RunInferenceDoFn(self._model_spec))
        #TODO If there is a beam.BatchElements, why no beam.UnBatchElements?
        | beam.FlatMap(_unbatch))


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
    logging.warning('Resource module is not available for current platform, '
                    'memory usage cannot be fetched.')
  return 0
