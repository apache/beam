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

"""Functionaliry for sampling elements during bundle execution."""

# pytype: skip-file

import collections
import logging
import threading
from threading import Timer
import time
from typing import Any
from typing import DefaultDict
from typing import Deque
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Union

from apache_beam.coders.coder_impl import CoderImpl
from apache_beam.coders.coder_impl import WindowedValueCoderImpl
from apache_beam.coders.coders import Coder
from apache_beam.utils.windowed_value import WindowedValue

_LOGGER = logging.getLogger(__name__)


class SampleTimer:
  def __init__(self, timeout, sampler):  # timeout in seconds
    self._timeout = timeout
    self._timer = Timer(self._timeout, self.sample)
    self._timer.start()
    self._sampler = sampler

  def reset(self):
    self._timer.cancel()
    self._timer = Timer(self._timeout, self.sample)
    self._timer.start()

  def stop(self):
    self._timer.cancel()

  def sample(self):
    self._sampler.sample()
    self.reset()


class ElementSampler:
  has_element: bool
  el: Any


class OutputSampler:
  """Represents a way to sample an output of a PTransform.

  This is configurable to only keep max_samples (see constructor) sampled
  elements in memory. The first 10 elements are always sampled, then after each
  sample_every_sec (see constructor).
  """
  def __init__(
      self,
      coder: Coder,
      max_samples: int = 10,
      sample_every_sec: float = 5,
      sample_timer_factory=None) -> None:
    self._samples: Deque[Any] = collections.deque(maxlen=max_samples)
    self._coder_impl: CoderImpl = coder.get_impl()
    self._sample_count: int = 0
    self._sample_every_sec: float = sample_every_sec

    sample_timer_factory = sample_timer_factory or SampleTimer
    self._sample_timer = sample_timer_factory(sample_every_sec, self)
    self.element_sampler = ElementSampler()

  def stop(self):
    self._sample_timer.stop()

  def remove_windowed_value(self, el: Union[WindowedValue, Any]) -> Any:
    """Retrieves the value from the WindowedValue.

    The Python SDK passes elements as WindowedValues, which may not match the
    coder for that particular PCollection.
    """
    if isinstance(el, WindowedValue):
      return self.remove_windowed_value(el.value)
    return el

  def flush(self) -> List[bytes]:
    """Returns all samples and clears buffer."""
    if isinstance(self._coder_impl, WindowedValueCoderImpl):
      samples = [s for s in self._samples]
    else:
      samples = [self.remove_windowed_value(s) for s in self._samples]

    # Encode in the nested context b/c this ensures that the SDK can decode the
    # bytes with the ToStringFn.
    self._samples.clear()
    return [self._coder_impl.encode_nested(s) for s in samples]

  def sample(self) -> None:
    """Samples the given element to an internal buffer.

    Samples are only taken for the first 10 elements then every
    `self._sample_every_sec` second after.
    """
    if self.element_sampler.has_element:
      self.element_sampler.has_element = False
      self._samples.append(self.element_sampler.el)


class DataSampler:
  """A class for querying any samples generated during execution.

  This class is meant to be a singleton with regard to a particular
  `sdk_worker.SdkHarness`. When creating the operators, individual
  `OutputSampler`s are created from `DataSampler.sample_output`. This allows for
  multi-threaded sampling of a PCollection across the SdkHarness.

  Samples generated during execution can then be sampled with the `samples`
  method. This filters samples from the given pcollection ids.
  """
  def __init__(
      self, max_samples: int = 10, sample_every_sec: float = 30, clock=None) -> None:
    # Key is PCollection id. Is guarded by the _samplers_lock.
    self._samplers: Dict[str, OutputSampler] = {}
    # Bundles are processed in parallel, so new samplers may be added when the
    # runner queries for samples.
    self._samplers_lock: threading.Lock = threading.Lock()
    self._max_samples = max_samples
    self._sample_every_sec = sample_every_sec
    self._element_samplers: Dict[str, List[ElementSampler]] = {}
    self._clock = clock

  def sample_exception(self,
      sample: ElementSampler,
      instruction_id: str,
      transform_id: str):
    _LOGGER.error('Sampled exception element "%s" from ' +
                  'instruction %s and transform %s' % (
        str(sample.el), instruction_id, transform_id))

  def stop(self):
    with self._samplers_lock:
      for sampler in self._samplers.values():
        sampler.stop()

  def sampler_for_output(self, transform_id, output_index):
    try:
      return self._element_samplers[transform_id][output_index]
    except:
      _LOGGER.warn('Out-of-bounds access for transform "%s" and output "%s" ' +
                   'ElementSampler. This may indicate that the transform was ' +
                   'improperly initialized with the DataSampler.' %
                   (transform_id, output_index))
      return ElementSampler()

  def initialize_samplers(self, transform_id, descriptor,
                           coder_factory, sample_timer_factory):
    transform_proto = descriptor.transforms[transform_id]
    with self._samplers_lock:
      for pcoll_id in transform_proto.outputs.values():
        if pcoll_id in self._samplers:
          continue

        coder_id = descriptor.pcollections[pcoll_id].coder_id
        coder = coder_factory(coder_id)

        sampler = OutputSampler(
            coder, self._max_samples, self._sample_every_sec,
            sample_timer_factory=sample_timer_factory)
        self._samplers[pcoll_id] = sampler

      if transform_id in self._element_samplers:
        return

      samplers = {
          pcoll_id: self._samplers[pcoll_id].element_sampler
          for pcoll_id in transform_proto.outputs.values()
      }

      tagged_samplers = {
          tag: samplers[pcoll_id]
          for tag, pcoll_id in transform_proto.outputs.items()
      }

      outputs = transform_proto.outputs
      indexed_samplers = [tagged_samplers[tag] for tag in outputs]
      self._element_samplers[transform_id] = indexed_samplers

  def samples(
      self,
      pcollection_ids: Optional[Iterable[str]] = None
  ) -> Dict[str, List[bytes]]:
    """Returns samples filtered PCollection ids.

    All samples from the given PCollections are returned. Empty lists are
    wildcards.
    """
    ret: DefaultDict[str, List[bytes]] = collections.defaultdict(lambda: [])

    with self._samplers_lock:
      samplers = self._samplers.copy()

    for pcoll_id in samplers:
      if pcollection_ids and pcoll_id not in pcollection_ids:
        continue

      samples = samplers[pcoll_id].flush()
      if samples:
        ret[pcoll_id].extend(samples)

    return dict(ret)
