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

from __future__ import annotations

import collections
import logging
import threading
from threading import Timer
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
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.utils.windowed_value import WindowedValue

_LOGGER = logging.getLogger(__name__)


class SampleTimer:
  """Periodic timer for sampling elements."""
  def __init__(self, timeout_secs: float, sampler: OutputSampler) -> None:
    self._timeout_secs = timeout_secs
    self._timer = Timer(self._timeout_secs, self.sample)
    self._sampler = sampler

  def reset(self):
    self._timer.cancel()
    self._timer = Timer(self._timeout_secs, self.sample)
    self._timer.start()

  def stop(self):
    self._timer.cancel()

  def sample(self):
    self._sampler.sample()
    self.reset()


class ElementSampler:
  """Record class to hold sampled elements.

  This class is used as an optimization to quickly sample elements. This is a
  shared reference between the Operation and the OutputSampler.
  """

  # Is true iff the `el` has been set with a sample.
  has_element: bool

  # The sampled element. Note that `None` is a valid element and cannot be uesd
  # as a sentintel to check if there is a sample. Use the `has_element` flag to
  # check for this case.
  el: Any


class OutputSampler:
  """Represents a way to sample an output of a PTransform.

  This is configurable to only keep `max_samples` (see constructor) sampled
  elements in memory. Samples are taken every `sample_every_sec`.
  """
  def __init__(
      self,
      coder: Coder,
      max_samples: int = 10,
      sample_every_sec: float = 5) -> None:
    self._samples: Deque[Any] = collections.deque(maxlen=max_samples)
    self._samples_lock: threading.Lock = threading.Lock()
    self._coder_impl: CoderImpl = coder.get_impl()
    self._sample_timer = SampleTimer(sample_every_sec, self)
    self.element_sampler = ElementSampler()
    self.element_sampler.has_element = False

    # For testing, it's easier to disable the Timer and manually sample.
    if sample_every_sec > 0:
      self._sample_timer.reset()

  def stop(self) -> None:
    """Stops sampling."""
    self._sample_timer.stop()

  def remove_windowed_value(self, el: Union[WindowedValue, Any]) -> Any:
    """Retrieves the value from the WindowedValue.

    The Python SDK passes elements as WindowedValues, which may not match the
    coder for that particular PCollection.
    """
    while isinstance(el, WindowedValue):
      el = el.value
    return el

  def flush(self, clear: bool = True) -> List[bytes]:
    """Returns all samples and optionally clears buffer if clear is True."""
    with self._samples_lock:
      if isinstance(self._coder_impl, WindowedValueCoderImpl):
        samples = [s for s in self._samples]
      else:
        samples = [self.remove_windowed_value(s) for s in self._samples]

      # Encode in the nested context b/c this ensures that the SDK can decode
      # the bytes with the ToStringFn.
      if clear:
        self._samples.clear()
      return [self._coder_impl.encode_nested(s) for s in samples]

  def sample(self) -> None:
    """Samples the given element to an internal buffer."""
    with self._samples_lock:
      if self.element_sampler.has_element:
        self.element_sampler.has_element = False
        self._samples.append(self.element_sampler.el)


class DataSampler:
  """A class for querying any samples generated during execution.

  This class is meant to be a singleton with regard to a particular
  `sdk_worker.SdkHarness`. When creating the operators, individual
  `OutputSampler`s are created from `DataSampler.initialize_samplers`. This
  allows for multi-threaded sampling of a PCollection across the SdkHarness.

  Samples generated during execution can then be sampled with the `samples`
  method. This filters samples from the given pcollection ids.
  """
  def __init__(
      self,
      max_samples: int = 10,
      sample_every_sec: float = 30,
      clock=None) -> None:
    # Key is PCollection id. Is guarded by the _samplers_lock.
    self._samplers: Dict[str, OutputSampler] = {}
    # Bundles are processed in parallel, so new samplers may be added when the
    # runner queries for samples.
    self._samplers_lock: threading.Lock = threading.Lock()
    self._max_samples = max_samples
    self._sample_every_sec = sample_every_sec
    self._element_samplers: Dict[str, List[ElementSampler]] = {}
    self._clock = clock

  def stop(self) -> None:
    """Stops all sampling, does not clear samplers in case there are outstanding
    samples.
    """
    with self._samplers_lock:
      for sampler in self._samplers.values():
        sampler.stop()

  def sampler_for_output(
      self, transform_id: str, output_index: int) -> ElementSampler:
    """Returns the ElementSampler for the given output."""
    try:
      return self._element_samplers[transform_id][output_index]
    except KeyError:
      _LOGGER.warning(
          f'Out-of-bounds access for transform "{transform_id}" ' +
          'and output "{output_index}" ElementSampler. This may ' +
          'indicate that the transform was improperly ' +
          'initialized with the DataSampler.')
      return ElementSampler()

  def initialize_samplers(
      self,
      transform_id: str,
      descriptor: beam_fn_api_pb2.ProcessBundleDescriptor,
      coder_factory) -> List[ElementSampler]:
    """Creates the OutputSamplers for the given PTransform.

    This initializes the samplers only once per PCollection Id. Note that an
    OutputSampler is created per PCollection and an ElementSampler is created
    per OutputSampler. This means that multiple ProcessBundles can and will
    share the same ElementSampler for a given PCollection.
    """
    transform_proto = descriptor.transforms[transform_id]
    with self._samplers_lock:
      # Initialize the samplers.
      for pcoll_id in transform_proto.outputs.values():
        # Only initialize new PCollections.
        if pcoll_id in self._samplers:
          continue

        # Create the sampler with the corresponding coder.
        coder_id = descriptor.pcollections[pcoll_id].coder_id
        coder = coder_factory(coder_id)
        sampler = OutputSampler(
            coder, self._max_samples, self._sample_every_sec)
        self._samplers[pcoll_id] = sampler

      # Next update the lookup table for ElementSamplers for a given PTransform.
      # Operations look up the ElementSampler for an output based on the index
      # of the tag in the PTransform's outputs. The following code intializes
      # the array with ElementSamplers in the correct indices.
      if transform_id in self._element_samplers:
        return self._element_samplers[transform_id]

      outputs = transform_proto.outputs
      samplers = [
          self._samplers[pcoll_id].element_sampler
          for pcoll_id in outputs.values()
      ]
      self._element_samplers[transform_id] = samplers

      return samplers

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
