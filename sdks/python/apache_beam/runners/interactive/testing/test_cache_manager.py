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
from __future__ import absolute_import

import collections
import itertools
import sys

import apache_beam as beam
from apache_beam import coders
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileHeader
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.runners.interactive.cache_manager import CacheManager
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp


class InMemoryCache(CacheManager):
  """A cache that stores all PCollections in an in-memory map.

  This is only used for checking the pipeline shape. This can't be used for
  running the pipeline isn't shared between the SDK and the Runner.
  """
  def __init__(self):
    self._cached = {}
    self._pcoders = {}

  def exists(self, *labels):
    return self._key(*labels) in self._cached

  def _latest_version(self, *labels):
    return True

  def read(self, *labels, **args):
    if not self.exists(*labels):
      return itertools.chain([]), -1

    return itertools.chain(self._cached[self._key(*labels)]), None

  def write(self, value, *labels):
    if not self.exists(*labels):
      self._cached[self._key(*labels)] = []
    self._cached[self._key(*labels)] += value

  def save_pcoder(self, pcoder, *labels):
    self._pcoders[self._key(*labels)] = pcoder

  def load_pcoder(self, *labels):
    return self._pcoders[self._key(*labels)]

  def cleanup(self):
    self._cached = collections.defaultdict(list)
    self._pcoders = {}

  def source(self, *labels):
    vals = self._cached[self._key(*labels)]
    return beam.Create(vals)

  def sink(self, labels, is_capture=False):
    return beam.Map(lambda _: _)

  def size(self, *labels):
    if self.exists(*labels):
      return sys.getsizeof(self._cached[self._key(*labels)])
    return 0

  def _key(self, *labels):
    return '/'.join([l for l in labels])


class NoopSink(beam.PTransform):
  def expand(self, pcoll):
    return pcoll | beam.Map(lambda x: x)


class FileRecordsBuilder(object):
  def __init__(self, tag=None):
    self._header = TestStreamFileHeader(tag=tag)
    self._records = []
    self._coder = coders.FastPrimitivesCoder()

  def add_element(self, element, event_time_secs):
    element_payload = TestStreamPayload.TimestampedElement(
        encoded_element=self._coder.encode(element),
        timestamp=Timestamp.of(event_time_secs).micros)
    record = TestStreamFileRecord(
        recorded_event=TestStreamPayload.Event(
            element_event=TestStreamPayload.Event.AddElements(
                elements=[element_payload])))
    self._records.append(record)
    return self

  def advance_watermark(self, watermark_secs):
    record = TestStreamFileRecord(
        recorded_event=TestStreamPayload.Event(
            watermark_event=TestStreamPayload.Event.AdvanceWatermark(
                new_watermark=Timestamp.of(watermark_secs).micros)))
    self._records.append(record)
    return self

  def advance_processing_time(self, delta_secs):
    record = TestStreamFileRecord(
        recorded_event=TestStreamPayload.Event(
            processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
                advance_duration=Duration.of(delta_secs).micros)))
    self._records.append(record)
    return self

  def build(self):
    return [self._header] + self._records
