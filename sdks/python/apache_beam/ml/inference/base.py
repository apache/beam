from typing import Any
from typing import Generic
from typing import Iterable
from typing import List
from typing import TypeVar
import logging
import resource
import platform
import sys
import time


import apache_beam as beam
from apache_beam.utils import shared
from apache_beam.ml.inference.apis import PredictionResult

_MILLISECOND_TO_MICROSECOND = 1000
_MICROSECOND_TO_NANOSECOND = 1000
_SECOND_TO_MICROSECOND = 1000000


def _unbatch(maybe_keyed_batches: Any):
  keys, results = maybe_keyed_batches
  if keys:
    return zip(keys, results)
  else:
    return results


ExampleType = TypeVar('ExampleType')
InferenceType = TypeVar('InferenceType')

# TODO: make model generic, right now, that causes a pickle error
class ModelLoader:
  """Has the ability to load an ML model."""

  def load_model(self):
    """Loads an initializes a model for processing."""
    raise NotImplementedError(type(self))

class InferenceRunner:
  "Implements runnning inferences."

  def run_inference(self, batch:Any, model: Any) -> List[PredictionResult]:
    """Runs inferences on a batch of examples and returns a list of Predictions."""
    raise NotImplementedError(type(self))


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
  def __init__(self, model_loader, inference_runner):
    # TODO: Handle model updated coming from side inputs.
    self._model_loader = model_loader
    self._inference_runner = inference_runner
    self._shared_model_handle = shared.Shared()
    # TODO: Compute a good metrics namespace
    self._metrics_collector = MetricsCollector('default_namespace')
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

    return self._shared_model_handle.acquire(load)


  def setup(self):
    super().setup()
    self._model = self._load_model()

  def process(self, batch):
    has_keys = isinstance(batch[0], tuple)
    start_time = self._clock.get_current_time_in_microseconds()
    if has_keys:
      examples = [example for _, example in batch]
      keys = [key for key, _ in batch]
    else:
      examples = batch
      keys = None
    inferences = self._inference_runner.run_inference(examples, self._model)
    inference_latency = self._clock.get_current_time_in_microseconds() - start_time
    num_bytes = sys.getsizeof(batch)
    num_elements = len(batch)
    self._metrics_collector.update(num_elements, sys.getsizeof(batch), inference_latency)
    yield keys, [PredictionResult(e, r) for e, r in zip(examples, inferences)]


class RunInferenceImpl(beam.PTransform):
  def __init__(self, model_loader, inference_runner):
    self._model_loader = model_loader
    self._inference_runner = inference_runner

  def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
    return (
        pcoll
        # TODO: Hook into the batching DoFn APIs.
        | beam.BatchElements()
        | beam.ParDo(RunInferenceDoFn(self._model_loader, self._inference_runner))
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


def _is_windows() -> bool:
  return platform.system() == 'Windows' or os.name == 'nt'


def _is_cygwin() -> bool:
  return platform.system().startswith('CYGWIN_NT')


class _Clock(object):

  def get_current_time_in_microseconds(self) -> int:
    return int(time.time() * _SECOND_TO_MICROSECOND)


class _FineGrainedClock(_Clock):

  def get_current_time_in_microseconds(self) -> int:
    return int(
        time.clock_gettime_ns(time.CLOCK_REALTIME) /  # pytype: disable=module-attr
        _MICROSECOND_TO_NANOSECOND)


class _ClockFactory(object):

  @staticmethod
  def make_clock() -> _Clock:
    if (hasattr(time, 'clock_gettime_ns') and not _is_windows() and
        not _is_cygwin()):
      return _FineGrainedClock()
    return _Clock()
