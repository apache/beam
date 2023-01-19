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

"""Class that allows for sampling of elements on a particular Operation."""

# pytype: skip-file
# mypy: disallow-untyped-defs

import collections
import logging
import threading
import time

from apache_beam.coders.coder_impl import WindowedValueCoderImpl
from apache_beam.utils.windowed_value import WindowedValue

_LOGGER = logging.getLogger(__name__)


class DataSampler:
  """"""

  def __init__(self):
    self._samplers = {}
    self._samplers_lock = threading.Lock()

  def sample_output(self, descriptor_id, pcoll_id, coder):
    with self._samplers_lock:
      key = (descriptor_id, pcoll_id)
      if key in self._samplers:
        sampler = self._samplers[key]
      else:
        sampler = OutputSampler(coder)
        self._samplers[key] = sampler
      return sampler

  def samples(self, descriptor_id=None, pcollections=None):
    ret = collections.defaultdict(lambda: {})

    with self._samplers_lock:
      samplers = self._samplers.copy()

    for sampler_id in samplers:
      sampler_descriptor_id, pcoll_id = sampler_id
      if descriptor_id and sampler_descriptor_id != descriptor_id:
        continue

      if pcollections and pcoll_id not in pcollections:
        continue

      samples = samplers[sampler_id].flush()
      if samples:
        ret[sampler_descriptor_id][pcoll_id] = samples

    return dict(ret)


class OutputSampler:

  def __init__(self, coder, max_samples=10, sample_every_n=1000):
    self._samples = collections.deque(maxlen=max_samples)
    self._coder_impl = coder.get_impl()
    self._sample_count = 0
    self._sample_every_n = sample_every_n

  def remove_windowed_value(self, el):
    if isinstance(el, WindowedValue):
      return self.remove_windowed_value(el.value)
    return el

  def flush(self):
    if isinstance(self._coder_impl, WindowedValueCoderImpl):
      samples = [s for s in self._samples]
    else:
      samples = [self.remove_windowed_value(s) for s in self._samples]

    self._samples.clear()
    return [self._coder_impl.encode(s) for s in samples]


  def sample(self, element):
    self._sample_count += 1

    if (self._sample_count <= 10 or
        self._sample_count % self._sample_every_n == 0):
      self._samples.append(element)

